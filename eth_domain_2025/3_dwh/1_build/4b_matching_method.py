import pandas as pd
import duckdb
from pathlib import Path

from utils import DB_FILE

# ---------- Config ----------
DB_PATH = Path(DB_FILE)
MATCH_ROOT = Path("../../2_matches")

INSTITUTIONS = {
    'empa': [71824836],
    'epfl': [5124864],
    'psi': [2802229908],
    'wsl': [2800262456],
    'ethz': [35440088],
    'eawag': [63664421],
}

# Columns from the matching output to keep
METRIC_COLS = [
    "metric_openalex_id_match",
    "metric_title_simhash",
    "metric_doi_match",
    "title_similarity",
    "rare_title",
    "journal_similarity",
    "authors_similarity",
    "flag_all_true",
    "same_year",
    "priority_score",
    "rule_FINAL",
]

# ---------- Connect ----------
con = duckdb.connect(str(DB_PATH))
con.execute("CREATE SCHEMA IF NOT EXISTS reports;")

# ---------- Table DDL ----------
con.execute("DROP TABLE IF EXISTS reports.report_matching_method;")
con.execute("""
CREATE TABLE reports.report_matching_method (
    report_name                VARCHAR,
    work_id_duplicated         BIGINT,
    metric_openalex_id_match   DOUBLE,
    metric_title_simhash       DOUBLE,
    metric_doi_match           DOUBLE,
    title_similarity           DOUBLE,
    rare_title                 BOOLEAN,
    journal_similarity         DOUBLE,
    authors_similarity         DOUBLE,
    flag_all_true              BOOLEAN,
    same_year                  BOOLEAN,
    priority_score             DOUBLE,
    rule_FINAL                 VARCHAR,
    source_file                VARCHAR
);
""")

con.execute("""
COMMENT ON TABLE reports.report_matching_method IS
'Per-work matching metrics and scores from the matching pipeline. Joins to report_membership via (report_name, original_line_number).';
COMMENT ON COLUMN reports.report_matching_method.report_name IS 'Institution/report name';
COMMENT ON COLUMN reports.report_matching_method.work_id_duplicated IS 'Matched OpenAlex work id (can be NULL for not_found)';
COMMENT ON COLUMN reports.report_matching_method.metric_openalex_id_match IS 'Whether OpenAlex ID matched directly';
COMMENT ON COLUMN reports.report_matching_method.metric_title_simhash IS 'SimHash-based title similarity metric';
COMMENT ON COLUMN reports.report_matching_method.metric_doi_match IS 'Whether DOI matched directly';
COMMENT ON COLUMN reports.report_matching_method.title_similarity IS 'Title similarity score (0-1)';
COMMENT ON COLUMN reports.report_matching_method.rare_title IS 'Whether the title is rare in the corpus';
COMMENT ON COLUMN reports.report_matching_method.journal_similarity IS 'Journal name similarity score (0-1)';
COMMENT ON COLUMN reports.report_matching_method.authors_similarity IS 'Author list similarity score (0-1)';
COMMENT ON COLUMN reports.report_matching_method.flag_all_true IS 'Whether all flag_ columns were True';
COMMENT ON COLUMN reports.report_matching_method.same_year IS 'Whether qpub_year == tpub_publication_year';
COMMENT ON COLUMN reports.report_matching_method.priority_score IS 'Composite priority score used for ranking matches';
COMMENT ON COLUMN reports.report_matching_method.rule_FINAL IS 'Final rule label (e.g. not_found, or NULL for matched)';
COMMENT ON COLUMN reports.report_matching_method.source_file IS 'Path to the source parquet file';
""")

# ---------- Load per institution ----------
for inst in INSTITUTIONS.keys():
    parquet_path = MATCH_ROOT / inst / "final_match" / "final_list_top_match.parquet"

    if not parquet_path.exists():
        print(f"⚠️  Skipping {inst}: file not found at {parquet_path}")
        continue

    df = pd.read_parquet(parquet_path)

    # Keep only the columns we need
    keep_cols = ["tpub_id"] + [c for c in METRIC_COLS if c in df.columns]
    df = df[[c for c in keep_cols if c in df.columns]].copy()

    df = df.rename(columns={
        "tpub_id": "work_id_duplicated",
    })

    df["work_id_duplicated"] = df["work_id_duplicated"].astype("Int64")

    df["report_name"] = inst
    df["source_file"] = str(parquet_path)

    con.register("df_load", df)
    con.execute("""
        INSERT INTO reports.report_matching_method
        SELECT
            report_name,
            work_id_duplicated::BIGINT,
            metric_openalex_id_match::DOUBLE,
            metric_title_simhash::DOUBLE,
            metric_doi_match::DOUBLE,
            title_similarity::DOUBLE,
            rare_title::BOOLEAN,
            journal_similarity::DOUBLE,
            authors_similarity::DOUBLE,
            flag_all_true::BOOLEAN,
            same_year::BOOLEAN,
            priority_score::DOUBLE,
            rule_FINAL::VARCHAR,
            source_file
        FROM df_load;
    """)
    con.unregister("df_load")

    n_rows = len(df)
    n_matched = int(df["work_id_duplicated"].notna().sum())
    print(f"✓ Loaded {n_rows:,} rows for '{inst}': {n_matched:,} matched, {n_rows - n_matched:,} not_found")

# ---------- Indexes ----------
con.execute("""
CREATE INDEX IF NOT EXISTS idx_matching_method_name_work
    ON reports.report_matching_method(report_name, work_id_duplicated);
""")

# ---------- Done ----------
con.execute("CHECKPOINT;")
con.close()

print("✓ Done — table reports.report_matching_method is ready")