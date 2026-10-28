from pathlib import Path
import duckdb

from utils import DB_FILE

DB_PATH = Path(DB_FILE)
con = duckdb.connect(str(DB_PATH))

con.execute("PRAGMA enable_object_cache=true;")
con.execute("PRAGMA memory_limit='30GB';")

con.execute("CREATE SCHEMA IF NOT EXISTS reports;")

con.execute(r"""
CREATE OR REPLACE VIEW marts.work_metrics AS
WITH patent_counts AS (
    SELECT
        work_id,
        COUNT(*) AS n_patent_citing
    FROM augmented.pub_patent
    GROUP BY work_id
)

SELECT
  -- Group-by primaries
  w.publication_year                           AS publication_year,
  w.subfield                                   AS subfield,
  w.type_is_proceedings                        AS type_is_proceedings,
  w.work_id                                    AS work_id,

  -- Core and OA profile
  w.type_is_core                               AS type_is_core,
  w.open_access_is_oa                          AS open_access_is_oa,
  w.open_access_oa_status                      AS open_access_oa_status,
  w.open_access_oa_status_is_closed            AS open_access_oa_status_is_closed,
  w.open_access_oa_status_is_bronze            AS open_access_oa_status_is_bronze,
  w.open_access_oa_status_is_green             AS open_access_oa_status_is_green,
  w.open_access_oa_status_is_hybrid            AS open_access_oa_status_is_hybrid,
  w.open_access_oa_status_is_diamond           AS open_access_oa_status_is_diamond,
  w.open_access_any_repository_has_fulltext    AS open_access_any_repository_has_fulltext,

  -- Reference / citation totals (full count)
  wrt.n_citations                              AS n_citations,
  wrt.n_references                             AS n_references,
  wrt.has_references                           AS has_references,

  -- Normalized citation metrics (full count)
  wcm.citations                                AS citations,
  wcm.n_institutions_on_work                   AS n_institutions_on_work,
  wcm.n_works                                  AS n_works,
  wcm.cohort_is_reliable                       AS cohort_is_reliable,
  wcm.is_cited                                 AS is_cited,
  wcm.is_uncited                               AS is_uncited,
  wcm.norm_vs_mean                             AS norm_vs_mean,
  wcm.norm_vs_median                           AS norm_vs_median,
  wcm.zscore                                   AS zscore,
  wcm.percentile_0_1                           AS percentile_0_1,
  wcm.decile                                   AS decile,
  wcm.is_top10                                 AS is_top10,

  -- Gender info
  g.number_male                                AS number_male,
  g.number_female                              AS number_female,
  g.number_unknown                             AS number_unknown,
  g.number_authors_on_work                     AS number_authors_on_work,
  g.fraction_male                              AS fraction_male,
  g.fraction_female                            AS fraction_female,
  g.fraction_unknown                           AS fraction_unknown,

  -- Patents citing this work
  pc.n_patent_citing                           AS n_patent_citing

FROM core.work w
LEFT JOIN marts.work_refcite_totals      wrt ON wrt.work_id = w.work_id
LEFT JOIN marts.work_citation_metrics    wcm ON wcm.work_id = w.work_id
LEFT JOIN marts.work_gender_info         g   ON g.work_id   = w.work_id
LEFT JOIN patent_counts                  pc  ON pc.work_id  = w.work_id
;
""")

con.execute("""
COMMENT ON VIEW marts.work_metrics IS
'Per-work metrics joined with citation totals, full-count normalized citation metrics, gender info, n_institutions_on_work (for later fractional aggregation), and number of citing patents.';
""")

con.close()