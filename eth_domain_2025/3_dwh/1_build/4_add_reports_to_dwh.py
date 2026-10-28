import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime

# ---------- Config ----------
from utils import DB_FILE

DB_PATH = Path(DB_FILE)
MATCH_ROOT = Path("../../2_matches")   # base folder for all institutions

INSTITUTIONS = {
    'empa': [71824836],
    'epfl': [5124864],
    'psi': [2802229908],
    'wsl': [2800262456],
    'ethz': [35440088],
    'eawag': [63664421],
    'ethdomain': [71824836, 5124864, 2802229908, 2800262456, 35440088, 63664421],
}


# ---------- Connect ----------
con = duckdb.connect(str(DB_PATH))
con.execute("CREATE SCHEMA IF NOT EXISTS reports;")

# ---------- Table DDL (with the new column) ----------
con.execute("DROP TABLE IF EXISTS reports.report_membership;")
con.execute("""
CREATE TABLE reports.report_membership (
  report_name            VARCHAR,
  original_line_number   INTEGER,
  work_id                BIGINT,       -- canonical assignment (only first occurrence per report_name)
  work_id_duplicated     BIGINT,       -- raw matched id; can repeat and stays populated
  source_file            VARCHAR
);
""")

# ---------- Comments ----------
con.execute("""
COMMENT ON TABLE  reports.report_membership IS
'Per-report membership rows from matching. work_id_duplicated is the raw matched id (can repeat). work_id is populated only for the first occurrence of a given (report_name, work_id_duplicated); later duplicates have work_id = NULL. This lets you keep full provenance while enforcing per-report uniqueness via WHERE work_id IS NOT NULL.';

COMMENT ON COLUMN reports.report_membership.report_name IS 'Logical report/institution name (e.g., epfl, ethz, psi...)';
COMMENT ON COLUMN reports.report_membership.original_line_number IS 'Line number from the input matching file to preserve provenance';
COMMENT ON COLUMN reports.report_membership.work_id IS 'Canonical unique assignment per report_name: set only on the first occurrence of a given work_id_duplicated; NULL for subsequent duplicates';
COMMENT ON COLUMN reports.report_membership.work_id_duplicated IS 'Raw matched work id from input; may appear multiple times for the same report_name';
COMMENT ON COLUMN reports.report_membership.source_file IS 'Absolute or relative path to the source parquet used to load this row';
""")

# ---------- Load per institution ----------
for inst in INSTITUTIONS.keys():
    parquet_path = MATCH_ROOT / inst / "final_match" / "final_list_top_match.parquet"

    if not parquet_path.exists():
        print(f"⚠️  Skipping {inst}: file not found at {parquet_path}")
        continue

    df = pd.read_parquet(parquet_path)

    # Normalize and keep needed columns
    rename_map = {
        "qpub_original_line_number": "original_line_number",
        "tpub_id": "work_id_duplicated",   # raw id goes here
    }
    df = df.rename(columns=rename_map)

    needed = ["original_line_number", "work_id_duplicated"]
    df = df[needed].copy()

    # Nullable integer types so NaN -> SQL NULL
    df["original_line_number"] = df["original_line_number"].astype("Int64")
    df["work_id_duplicated"] = df["work_id_duplicated"].astype("Int64")

    # Mark the first occurrence of each non-null work_id_duplicated as representative
    # - First occurrence per inst is the one with the smallest original_line_number after sort
    df_sorted = df.sort_values(["work_id_duplicated", "original_line_number"], kind="mergesort")
    is_rep = (~df_sorted["work_id_duplicated"].isna()) & (~df_sorted.duplicated(subset=["work_id_duplicated"], keep="first"))

    # Canonical work_id: only set for the representative rows; NULL otherwise (and for null raw ids)
    df_sorted["work_id"] = pd.Series(pd.NA, index=df_sorted.index, dtype="Int64")
    df_sorted.loc[is_rep, "work_id"] = df_sorted.loc[is_rep, "work_id_duplicated"]

    # Insert rows (both columns)
    con.register("df_load", df_sorted)
    con.execute("""
        INSERT INTO reports.report_membership
        SELECT
            ?,                                   -- report_name
            original_line_number::INTEGER,
            work_id::BIGINT,                     -- canonical: only on first occurrence
            work_id_duplicated::BIGINT,          -- raw matched id (can repeat)
            ?
        FROM df_load;
    """, [inst, str(parquet_path)])
    con.unregister("df_load")

    # Stats
    n_rows = len(df_sorted)
    n_unique = int(df_sorted["work_id"].notna().sum())
    n_dups = int(df_sorted["work_id_duplicated"].notna().sum() - n_unique)
    print(f"✓ Loaded {n_rows:,} rows for '{inst}': {n_unique:,} unique works, {n_dups:,} duplicate occurrences")

# ---------- Indexes ----------
con.execute("""
CREATE INDEX IF NOT EXISTS idx_reports_name_work
  ON reports.report_membership(report_name, work_id);
""")
con.execute("""
CREATE INDEX IF NOT EXISTS idx_reports_name_workdup
  ON reports.report_membership(report_name, work_id_duplicated);
""")
con.execute("""
CREATE INDEX IF NOT EXISTS idx_reports_name_line
  ON reports.report_membership(report_name, original_line_number);
""")



# ---------- Keep institution IDs per report ----------
con.execute("""
CREATE TABLE IF NOT EXISTS reports.report_institutions_ids (
    report_name     VARCHAR,
    institution_id  BIGINT
);
""")

# Unique + efficient join key
con.execute("""
CREATE UNIQUE INDEX IF NOT EXISTS idx_report_inst_ids_unique
    ON reports.report_institutions_ids(report_name, institution_id);
""")

# Store the openalex ids of the institutions
con.execute("""
CREATE INDEX IF NOT EXISTS idx_report_inst_ids_inst
    ON reports.report_institutions_ids(institution_id);
""")
# Store the openalex ids of the institutions
con.execute("""
CREATE INDEX IF NOT EXISTS idx_report_work
  ON reports.report_membership(work_id);
""")
all_names = list(INSTITUTIONS.keys())
placeholders = ",".join(["?"] * len(all_names))
con.execute(
    f"DELETE FROM reports.report_institutions_ids "
    f"WHERE report_name IN ({placeholders})",
    all_names
)
rows = [
    (name, int(inst_id))
    for name, ids in INSTITUTIONS.items()
    for inst_id in ids
]

df_inst = pd.DataFrame(rows, columns=["report_name", "institution_id"])
con.register("df_inst", df_inst)
con.execute("""
    INSERT INTO reports.report_institutions_ids
    SELECT report_name, institution_id::BIGINT
    FROM df_inst;
""")
con.unregister("df_inst")

print("✓ Updated reports.report_institutions_ids mapping")



con.execute("CHECKPOINT;")
con.close()