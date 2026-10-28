import duckdb
from pathlib import Path

from utils import DB_FILE

DB_PATH = Path(DB_FILE)
con = duckdb.connect(str(DB_FILE))

con.execute("""
CREATE SCHEMA IF NOT EXISTS marts;

-- ------------------------------------------
-- Distinct (work_id, institution_id) per work
-- ------------------------------------------
CREATE OR REPLACE TEMP TABLE _work_institution AS
SELECT
  wai.work_id,
  wai.institution_id
FROM core.work_author_inst AS wai
WHERE wai.institution_id IS NOT NULL
GROUP BY wai.work_id, wai.institution_id;

-- ------------------------------------------
-- Count institutions per work
-- ------------------------------------------
CREATE OR REPLACE TEMP TABLE _n_institutions AS
SELECT
  work_id,
  COUNT(*)::INTEGER AS n_institutions_on_work
FROM _work_institution
GROUP BY work_id;

-- ------------------------------------------
-- Final mart: one row per (work, institution_id)
-- ------------------------------------------
CREATE OR REPLACE TABLE marts.work_institution_info AS
SELECT
  wi.work_id                              AS work_id,
  wi.institution_id                       AS institution_id,
  n.n_institutions_on_work                AS n_institutions_on_work,
  1.0::DOUBLE                             AS weight_full,
  CASE
    WHEN n.n_institutions_on_work > 0
      THEN (1.0::DOUBLE / n.n_institutions_on_work)
    ELSE NULL
  END                                     AS weight_frac
FROM _work_institution wi
JOIN _n_institutions n USING (work_id)
JOIN core.work w ON w.work_id = wi.work_id;
""")

# Indexes for speed
con.execute("""
CREATE UNIQUE INDEX IF NOT EXISTS idx_wiw_pk
ON marts.work_institution_info(work_id, institution_id);
""")
con.execute("""
CREATE INDEX IF NOT EXISTS idx_wiw_inst
ON marts.work_institution_info(institution_id);
""")

con.execute("""
COMMENT ON TABLE marts.work_institution_info IS
'One row per (work, institution_id). Provides institution-level full (1.0) and fractional (1/n_institutions_on_work) weights. Join to core.dim_institution for name, type, country_id.';
""")

con.execute("CHECKPOINT;")
con.close()

print("✓ Built marts.work_institution_info (work_id × institution_id with fractional weights)")