# 3_dwh

Build the DuckDB data warehouse used to compute the indicators of the reports. Run the scripts in [`1_build/`](1_build) from that folder, in numerical order, with one exception: `4_add_reports_to_dwh.py` must run before `3b_call_api_for_gender.py`. The database path is set in [`1_build/utils.py`](1_build/utils.py).

The warehouse covers the **full OpenAlex snapshot**, so that citation impact can be normalised against all works worldwide. Only the `reports` schema is specific to the ETH Domain (which works belong to which report). The schema is shown in [`2_diagram/`](2_diagram).

## Build steps

| Script | Output |
| --- | --- |
| `1_build_staging_area.py` | Normalised Parquet tables in `1_staging_area/`, built from the snapshot. Applies the scope filter: `work_to_status` is written for every work, the other tables only for in-scope works. |
| `2_build_dwh_from_staging.py` | `core.*` tables (works, authorships, references, authors, institutions, countries) with indexes |
| `2b_build_topic_table.py` | `core.topics_tree` (topic › subfield › field › domain) |
| `3_add_patents_to_dwh.py` | `augmented.pub_patent` (patents citing a work) |
| `3b_call_api_for_gender.py` | Genderize API results cached in `gender/kv/`, starting with the authors of the listed publications, then all authors of in-scope works from 2014–2023. Needs `GENDERIZE_API_KEY` in `.env` and `reports.report_membership`. Resumable. |
| `3c_add_gender_to_dwh.py` | `augmented.author_gender` |
| `3d_add_countries_details.py` | `augmented.openalex_countries` (country names and continents) |
| `4_add_reports_to_dwh.py` | `reports.report_membership`, `reports.report_institutions_ids`, from the matches of `2_matches` |
| `4b_matching_method.py` | `reports.report_matching_method` (how each publication was matched) |
| `5_1_marts_*.py` | `marts.work_refcite_totals`, `marts.work_institution_info`, `marts.work_country_info` (full and fractional weights), `marts.work_gender_info`, `reports.work_author_gender` |
| `5_2_marts_citation_stats.py` | `marts.citation_stats`: citation mean, median, standard deviation and 90th percentile per cohort |
| `5_3_marts_work_citation_metrics.py` | `marts.work_citation_metrics`: normalised citation score (basis of MNCS), percentile, top 10% flag |
| `6_views_work_metrics.py` | `marts.work_metrics`: one view joining the per-work metrics |

Citation metrics are normalised within cohorts of works with the same publication year, OpenAlex subfield and proceedings flag. Citations are those made by in-scope works, up to the snapshot date (no fixed citation window). Cohorts with fewer than 500 works are flagged as unreliable (`cohort_is_reliable`).

## Gender

OpenAlex does not provide author gender. We infer it from the first name with [Genderize.io](https://genderize.io): an author is counted as male or female only if the probability is above 0.65, and as unknown otherwise (including names reduced to an initial).
