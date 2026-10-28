import duckdb
from pathlib import Path


from utils import DB_FILE

DB_PATH = Path(DB_FILE)
con = duckdb.connect(str(DB_FILE))

# Optional tuning
con.execute("PRAGMA threads=4;")
con.execute("PRAGMA enable_object_cache=true;")
con.execute("PRAGMA memory_limit='36GB';")

con.execute("BEGIN;")
con.execute("CREATE SCHEMA IF NOT EXISTS marts;")

con.execute("""
-- ------------------------------------------
-- Distinct (work_id, author_id)
-- ------------------------------------------
CREATE OR REPLACE TEMP TABLE _work_authors AS
SELECT DISTINCT
  wai.work_id,
  wai.author_id
FROM core.work_author_inst AS wai
WHERE wai.author_id IS NOT NULL;

-- ------------------------------------------
-- Join with gender table and apply rules
-- ------------------------------------------
CREATE OR REPLACE TEMP TABLE _work_authors_gender AS
SELECT
  wa.work_id,
  wa.author_id,
  CASE
    WHEN ag.initial_only THEN 'unknown'
    WHEN ag.probability IS NULL OR ag.probability <= 0.65 THEN 'unknown'
    WHEN LOWER(ag.gender) IN ('male','female') THEN LOWER(ag.gender)
    ELSE 'unknown'
  END AS gender_effective
FROM _work_authors wa
LEFT JOIN augmented.author_gender ag
  USING (author_id);

-- ------------------------------------------
-- Aggregate counts per work
-- ------------------------------------------
CREATE OR REPLACE TABLE marts.work_gender_info AS
WITH counts AS (
  SELECT
    work_id,
    SUM(CASE WHEN gender_effective = 'male'   THEN 1 ELSE 0 END)::INTEGER AS number_male,
    SUM(CASE WHEN gender_effective = 'female' THEN 1 ELSE 0 END)::INTEGER AS number_female,
    SUM(CASE WHEN gender_effective = 'unknown' THEN 1 ELSE 0 END)::INTEGER AS number_unknown
  FROM _work_authors_gender
  GROUP BY work_id
)
SELECT
  c.work_id,
  c.number_male,
  c.number_female,
  c.number_unknown,
  (c.number_male + c.number_female + c.number_unknown)::INTEGER AS number_authors_on_work,
  CASE
    WHEN (c.number_male + c.number_female + c.number_unknown) > 0
      THEN c.number_male::DOUBLE / (c.number_male + c.number_female + c.number_unknown)
    ELSE NULL
  END AS fraction_male,
  CASE
    WHEN (c.number_male + c.number_female + c.number_unknown) > 0
      THEN c.number_female::DOUBLE / (c.number_male + c.number_female + c.number_unknown)
    ELSE NULL
  END AS fraction_female,
  CASE
    WHEN (c.number_male + c.number_female + c.number_unknown) > 0
      THEN c.number_unknown::DOUBLE / (c.number_male + c.number_female + c.number_unknown)
    ELSE NULL
  END AS fraction_unknown
FROM counts c
JOIN core.work w ON w.work_id = c.work_id;  -- keep discipline of restricting to known works
""")

# Index and comments
con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_work_gender_info_pk ON marts.work_gender_info(work_id);")

con.execute("""
COMMENT ON TABLE  marts.work_gender_info IS
'Per-work gender counts using augmented.author_gender with probability > 0.65; initial_only and low-confidence are unknown.';
COMMENT ON COLUMN marts.work_gender_info.work_id                  IS 'Work identifier';
COMMENT ON COLUMN marts.work_gender_info.number_male              IS 'Count of authors inferred as male';
COMMENT ON COLUMN marts.work_gender_info.number_female            IS 'Count of authors inferred as female';
COMMENT ON COLUMN marts.work_gender_info.number_unknown           IS 'Count of authors with unknown gender (low confidence, initial only, or missing)';
COMMENT ON COLUMN marts.work_gender_info.number_authors_on_work   IS 'Total distinct authors on the work';
COMMENT ON COLUMN marts.work_gender_info.fraction_male            IS 'number_male divided by total authors';
COMMENT ON COLUMN marts.work_gender_info.fraction_female          IS 'number_female divided by total authors';
COMMENT ON COLUMN marts.work_gender_info.fraction_unknown         IS 'number_unknown divided by total authors';
""")

con.execute("ANALYZE;")
con.execute("COMMIT;")
con.execute("CHECKPOINT;")
con.close()
print("✓ marts.work_gender_info built")