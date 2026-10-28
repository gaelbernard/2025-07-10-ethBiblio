import duckdb
from pathlib import Path

from utils import DB_FILE

DB_PATH = Path(DB_FILE)
threshold = 500   # minimum cohort size considered "reliable"

con = duckdb.connect(str(DB_FILE))

con.execute("PRAGMA threads=4;")
con.execute("PRAGMA enable_object_cache=true;")
con.execute("PRAGMA memory_limit='36GB';")

con.execute("BEGIN;")
con.execute("CREATE SCHEMA IF NOT EXISTS marts;")

con.execute(f"""
CREATE OR REPLACE TABLE marts.work_citation_metrics AS
WITH inst_counts AS (
  -- One row per work_id with number of institutions (avoid duplicates)
  SELECT
    work_id,
    MAX(n_institutions_on_work) AS n_institutions_on_work
  FROM marts.work_institution_info
  GROUP BY work_id
),
base AS (
  SELECT
    w.work_id,
    w.publication_year,
    w.subfield,
    w.type_is_proceedings,
    wrct.n_citations::BIGINT AS citations,  -- full-count citations
    COALESCE(ic.n_institutions_on_work, 1) AS n_institutions_on_work
  FROM marts.work_refcite_totals wrct
  JOIN core.work w USING (work_id)
  LEFT JOIN inst_counts ic USING (work_id)
),
joined AS (
  SELECT
    b.*,
    s.n_works,
    s.mean_citations,
    s.median_citations,
    s.stdev_citations,
    s.p90
  FROM base b
  JOIN marts.citation_stats s
    USING (publication_year, subfield, type_is_proceedings)
),
ranked AS (
  SELECT
    j.*,

    -- rankings / percentiles for full-count citations
    CUME_DIST() OVER (
      PARTITION BY publication_year, subfield, type_is_proceedings
      ORDER BY citations
    ) AS pctile_asc,
    NTILE(10) OVER (
      PARTITION BY publication_year, subfield, type_is_proceedings
      ORDER BY citations DESC
    ) AS decile_desc,
    RANK() OVER (
      PARTITION BY publication_year, subfield, type_is_proceedings
      ORDER BY citations DESC
    ) AS rank_in_group
  FROM joined j
)
SELECT
  work_id,
  publication_year,
  subfield,
  type_is_proceedings,

  -- raw citation counts
  citations,              -- full-count citations
  n_institutions_on_work, -- denominator to use later for fractional aggregation

  n_works,

  (n_works >= {threshold})::BOOLEAN                  AS cohort_is_reliable,

  (citations > 0)::BOOLEAN                           AS is_cited,
  (citations = 0)::BOOLEAN                           AS is_uncited,

  -- full-count normalized metrics
  (citations::DOUBLE / NULLIF(mean_citations, 0))           AS norm_vs_mean,
  (citations::DOUBLE / NULLIF(median_citations, 0))         AS norm_vs_median,
  ((citations - mean_citations) / NULLIF(stdev_citations, 0)) AS zscore,

  pctile_asc                                         AS percentile_0_1,
  decile_desc                                        AS decile,
  (citations >= p90)::BOOLEAN                        AS is_top10

FROM ranked;
""")

# Indexes to support typical access patterns
con.execute("""
CREATE INDEX IF NOT EXISTS idx_work_citation_metrics_work
  ON marts.work_citation_metrics(work_id);
""")
con.execute("""
CREATE INDEX IF NOT EXISTS idx_work_citation_metrics_grp
  ON marts.work_citation_metrics(publication_year, subfield, type_is_proceedings);
""")

# Clear and precise documentation
con.execute(f"""
COMMENT ON TABLE  marts.work_citation_metrics IS
'Per-work citation metrics in FULL COUNTING, normalized within (publication_year, subfield, type_is_proceedings) cohorts. \
Use n_institutions_on_work together with marts.work_institution_info to compute institutional full vs fractional MNCS at aggregation time. \
Metrics are computed for all works; use cohort_is_reliable (n_works >= {threshold}) to filter stable cohorts. \
Sources: marts.work_refcite_totals, marts.work_institution_info, marts.citation_stats.';

COMMENT ON COLUMN marts.work_citation_metrics.work_id IS 'Work ID';
COMMENT ON COLUMN marts.work_citation_metrics.publication_year IS 'Publication year of the work';
COMMENT ON COLUMN marts.work_citation_metrics.subfield IS 'Field/subfield used for cohort grouping';
COMMENT ON COLUMN marts.work_citation_metrics.type_is_proceedings IS 'Proceedings flag used in grouping';

COMMENT ON COLUMN marts.work_citation_metrics.citations IS 'Total citations for the work (full count, 0 if none), from marts.work_refcite_totals';
COMMENT ON COLUMN marts.work_citation_metrics.n_institutions_on_work IS 'Number of institutions on the work (default 1 when unknown)';

COMMENT ON COLUMN marts.work_citation_metrics.n_works IS 'Cohort size used for reliability flag';
COMMENT ON COLUMN marts.work_citation_metrics.cohort_is_reliable IS 'True if n_works >= threshold; recommended filter for normalized metrics';

COMMENT ON COLUMN marts.work_citation_metrics.is_cited IS 'True if full-count citations > 0';
COMMENT ON COLUMN marts.work_citation_metrics.is_uncited IS 'True if full-count citations = 0';

COMMENT ON COLUMN marts.work_citation_metrics.norm_vs_mean IS 'Full-count: citations / mean_citations within cohort (NULL if mean_citations = 0)';
COMMENT ON COLUMN marts.work_citation_metrics.norm_vs_median IS 'Full-count: citations / median_citations within cohort (NULL if median_citations = 0)';
COMMENT ON COLUMN marts.work_citation_metrics.zscore IS 'Full-count: (citations - mean_citations) / stdev_citations within cohort (NULL if stdev_citations = 0)';
COMMENT ON COLUMN marts.work_citation_metrics.percentile_0_1 IS 'Full-count: cume-dist percentile [0..1], ascending by citations';
COMMENT ON COLUMN marts.work_citation_metrics.decile IS 'Full-count: decile 1..10, 1 is top decile';
COMMENT ON COLUMN marts.work_citation_metrics.is_top10 IS 'Full-count: True if citations >= p90 within cohort';
""")

con.execute("ANALYZE;")
con.execute("COMMIT;")
con.execute("CHECKPOINT;")
con.close()
print("✓ marts.work_citation_metrics built (full-count metrics, n_institutions_on_work kept for fractional aggregation)")