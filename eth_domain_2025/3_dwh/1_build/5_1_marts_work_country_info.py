import duckdb
from pathlib import Path

from utils import DB_FILE

DB_PATH = Path(DB_FILE)
con = duckdb.connect(str(DB_FILE))

con.execute("""
CREATE SCHEMA IF NOT EXISTS marts;

-- ------------------------------------------
-- Distinct (work_id, country_id) per work
-- ------------------------------------------
CREATE OR REPLACE TEMP TABLE _work_country AS
SELECT
  wai.work_id,
  di.country_id
FROM core.work_author_inst AS wai
JOIN core.dim_institution AS di USING (institution_id)
WHERE di.country_id IS NOT NULL
GROUP BY wai.work_id, di.country_id;

-- ------------------------------------------
-- Count countries per work
-- ------------------------------------------
CREATE OR REPLACE TEMP TABLE _n_countries AS
SELECT
  work_id,
  COUNT(*)::INTEGER AS n_countries_on_work
FROM _work_country
GROUP BY work_id;

-- ------------------------------------------
-- Final mart: one row per (work, country_id)
-- ------------------------------------------
CREATE OR REPLACE TABLE marts.work_country_info AS
SELECT
  wc.work_id                            AS work_id,
  wc.country_id                         AS country_id,
  n.n_countries_on_work                 AS n_countries_on_work,
  1.0::DOUBLE                           AS country_weight_full,
  CASE
    WHEN n.n_countries_on_work > 0
      THEN (1.0::DOUBLE / n.n_countries_on_work)
    ELSE NULL
  END                                   AS country_weight_frac
FROM _work_country wc
JOIN _n_countries n USING (work_id)
JOIN core.work w ON w.work_id = wc.work_id;
""")

# Index for speed (use numeric key)
con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_wci_pk ON marts.work_country_info(work_id, country_id);")

con.execute("CHECKPOINT;")

con.close()
print("✓ Built marts.work_country_info (work_id × country_id with fractional weights)")