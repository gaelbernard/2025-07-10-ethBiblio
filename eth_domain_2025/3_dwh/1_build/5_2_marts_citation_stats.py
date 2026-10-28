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

# -------------------------------------------------------------------------
# Build PURE full-count citation stats per cohort
# -------------------------------------------------------------------------
con.execute("""
CREATE OR REPLACE TABLE marts.citation_stats AS
SELECT
  w.publication_year,
  w.subfield,
  w.type_is_proceedings,

  COUNT(*)::BIGINT                                AS n_works,

  AVG(wrct.n_citations::DOUBLE)                   AS mean_citations,
  MEDIAN(wrct.n_citations::DOUBLE)                AS median_citations,
  STDDEV_SAMP(wrct.n_citations::DOUBLE)           AS stdev_citations,
  QUANTILE_CONT(wrct.n_citations::DOUBLE, 0.90)   AS p90

FROM core.work w
JOIN marts.work_refcite_totals wrct USING (work_id)
GROUP BY w.publication_year, w.subfield, w.type_is_proceedings;
""")

con.execute("""
CREATE INDEX IF NOT EXISTS idx_citation_stats_grp
  ON marts.citation_stats(publication_year, subfield, type_is_proceedings);
""")

# -------------------------------------------------------------------------
# Comments
# -------------------------------------------------------------------------
con.execute("""
COMMENT ON TABLE marts.citation_stats IS
'Cohort-level citation statistics computed in FULL COUNTING only.
These values form the reference set for MNCS calculations.
To get institutional fractional MNCS, apply fractional weights at aggregation time.';

COMMENT ON COLUMN marts.citation_stats.publication_year IS 'Cohort year';
COMMENT ON COLUMN marts.citation_stats.subfield IS 'Cohort subfield';
COMMENT ON COLUMN marts.citation_stats.type_is_proceedings IS 'Proceedings flag for cohort grouping';

COMMENT ON COLUMN marts.citation_stats.n_works IS 'Number of works in the cohort';

COMMENT ON COLUMN marts.citation_stats.mean_citations IS 'Average citations in cohort (full count)';
COMMENT ON COLUMN marts.citation_stats.median_citations IS 'Median citations in cohort (full count)';
COMMENT ON COLUMN marts.citation_stats.stdev_citations IS 'Standard deviation of citations in cohort (full count)';
COMMENT ON COLUMN marts.citation_stats.p90 IS '90th percentile of citations (full count)';
""")

con.execute("ANALYZE;")
con.execute("COMMIT;")
con.execute("CHECKPOINT;")

con.close()
print("✓ marts.citation_stats rebuilt (full-count only)")