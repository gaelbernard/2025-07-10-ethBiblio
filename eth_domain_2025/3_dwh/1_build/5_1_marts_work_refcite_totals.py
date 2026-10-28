import duckdb
from pathlib import Path

from utils import DB_FILE

DB_PATH = Path(DB_FILE)
con = duckdb.connect(str(DB_FILE))

con.execute("PRAGMA threads=4;")
con.execute("PRAGMA enable_object_cache=true;")
con.execute("PRAGMA memory_limit='36GB';")

con.execute("BEGIN;")
con.execute("CREATE SCHEMA IF NOT EXISTS marts;")

# Build counts once
con.execute("""
CREATE OR REPLACE TEMP TABLE _cited AS
SELECT cited_id AS work_id, COUNT(*)::BIGINT AS n_citations
FROM core.refs
GROUP BY cited_id;

CREATE OR REPLACE TEMP TABLE _citing AS
SELECT citing_id AS work_id, COUNT(*)::BIGINT AS n_references
FROM core.refs
GROUP BY citing_id;
""")

# All works with COALESCE to include zeros
con.execute("""
CREATE OR REPLACE TABLE marts.work_refcite_totals AS
SELECT
  w.work_id,
  COALESCE(c.n_citations,  0)::BIGINT AS n_citations,
  COALESCE(r.n_references, 0)::BIGINT AS n_references,
  (COALESCE(c.n_citations,  0) > 0)   AS is_cited,
  (COALESCE(r.n_references, 0) > 0)   AS has_references
FROM core.work w
LEFT JOIN _cited  c ON c.work_id = w.work_id
LEFT JOIN _citing r ON r.work_id = w.work_id;
""")

con.execute("CREATE INDEX IF NOT EXISTS idx_wrct_work ON marts.work_refcite_totals(work_id);")

con.execute("""
COMMENT ON TABLE  marts.work_refcite_totals IS
'All works with incoming (n_citations) and outgoing (n_references) counts from core.refs. Counts are 0 when none. Booleans is_cited / has_references indicate > 0.';
COMMENT ON COLUMN marts.work_refcite_totals.work_id        IS 'Work identifier';
COMMENT ON COLUMN marts.work_refcite_totals.n_citations    IS 'Incoming references: count over core.refs.cited_id (0 if none)';
COMMENT ON COLUMN marts.work_refcite_totals.n_references   IS 'Outgoing references: count over core.refs.citing_id (0 if none)';
COMMENT ON COLUMN marts.work_refcite_totals.is_cited       IS 'True if n_citations > 0';
COMMENT ON COLUMN marts.work_refcite_totals.has_references IS 'True if n_references > 0';
""")

con.execute("ANALYZE;")
con.execute("COMMIT;")
con.execute("CHECKPOINT;")
con.close()
print("✓ marts.work_refcite_totals built")