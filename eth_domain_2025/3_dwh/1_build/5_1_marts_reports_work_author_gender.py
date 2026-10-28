import duckdb
from pathlib import Path

from utils import DB_FILE

DB_PATH = Path(DB_FILE)
con = duckdb.connect(str(DB_FILE))

# ---------- PRAGMAs ----------
con.execute("PRAGMA threads=4;")
con.execute("PRAGMA enable_object_cache=true;")
con.execute("PRAGMA memory_limit='36GB';")

con.execute("BEGIN;")
con.execute("CREATE SCHEMA IF NOT EXISTS reports;")

# ---------- Build the mart ----------
con.execute("""
CREATE OR REPLACE TABLE reports.work_author_gender AS
SELECT
    wai.work_id::BIGINT                                  AS work_id,
    wai.author_id::BIGINT                                AS author_id,
    wai.institution_id::BIGINT                           AS institution_id,
    wai.is_first_author                          AS is_first_author,
    wai.is_last_author                           AS is_last_author,

    CASE
        WHEN ag.initial_only THEN 'unknown'
        WHEN ag.probability IS NULL OR ag.probability <= 0.65 THEN 'unknown'
        WHEN LOWER(ag.gender) IN ('male','female') THEN LOWER(ag.gender)
        ELSE 'unknown'
    END                                           AS gender_effective,

    -- TRUE if the author's institution is one of the institutions
    -- associated with that report_name
    (rii.institution_id IS NOT NULL)              AS assigned_to_institution,   

    rep.report_name                               AS report_name

FROM core.work_author_inst AS wai
JOIN augmented.author_gender AS ag
    ON ag.author_id = wai.author_id
JOIN reports.report_membership AS rep
    ON rep.work_id = wai.work_id
LEFT JOIN reports.report_institutions_ids AS rii
    ON rii.report_name = rep.report_name
   AND rii.institution_id = wai.institution_id
;
""")

# ---------- Indexes ----------
con.execute("""
CREATE INDEX IF NOT EXISTS idx_wag_report_work
  ON reports.work_author_gender(report_name, work_id);
""")

con.execute("""
CREATE INDEX IF NOT EXISTS idx_wag_work_author
  ON reports.work_author_gender(work_id, author_id);
""")

con.execute("""
CREATE INDEX IF NOT EXISTS idx_wag_inst
  ON reports.work_author_gender(institution_id);
""")

# ---------- Comments ----------
con.execute("""
COMMENT ON TABLE reports.work_author_gender IS
'Per-(work, author) gender attribution with institutional assignment. Includes gender_effective and whether the author is assigned to the report''s institution(s).';

COMMENT ON COLUMN reports.work_author_gender.work_id                IS 'Work identifier';
COMMENT ON COLUMN reports.work_author_gender.author_id              IS 'Author identifier';
COMMENT ON COLUMN reports.work_author_gender.institution_id         IS 'Institution associated with this author on this work';
COMMENT ON COLUMN reports.work_author_gender.is_first_author        IS 'True if this author is first author';
COMMENT ON COLUMN reports.work_author_gender.is_last_author         IS 'True if this author is last author';
COMMENT ON COLUMN reports.work_author_gender.gender_effective       IS 'Gender category (male, female, unknown) based on thresholds and initial-only flag';
COMMENT ON COLUMN reports.work_author_gender.assigned_to_institution IS 'True if institution_id belongs to the report''s institutions';
COMMENT ON COLUMN reports.work_author_gender.report_name            IS 'Logical report name (epfl, ethz, ethdomain, etc.)';
""")

con.execute("ANALYZE;")
con.execute("COMMIT;")
con.execute("CHECKPOINT;")
con.close()

print("✓ reports.work_author_gender built")