# 2_build_dwh.py
# Build a DuckDB data warehouse from staging Parquet with:
# - Numeric country_id mapped from country_code (core.dim_country via DENSE_RANK)
# - Single transaction for CTAS, then staged index builds with spill + checkpoints
# - Parallel ANALYZE
# - Types: BIGINT for work_id, author_id, institution_id

import duckdb
from pathlib import Path
from utils import DB_FILE, DUCKDB_TMP

# ---------- Config ----------
STAGING_DIR = Path("1_staging_area")
TMP_DIR     = str(DUCKDB_TMP)

# Parquet paths
P_WORK              = str(STAGING_DIR / "work" / "*.parquet")
P_WORK_TO_STATUS    = str(STAGING_DIR / "work_to_status" / "*.parquet")
P_WORK_TO_REFS      = str(STAGING_DIR / "work_to_refs" / "*.parquet")
P_WORK_AUTHOR_INST  = str(STAGING_DIR / "work_author_inst" / "*.parquet")
P_DIM_AUTHOR        = str(STAGING_DIR / "dim_author" / "*.parquet")
P_DIM_INSTITUTION   = str(STAGING_DIR / "dim_institution" / "*.parquet")

# ---------- Checks ----------
missing_dirs = [
    p for p in [
        STAGING_DIR / "work",
        STAGING_DIR / "work_to_status",
        STAGING_DIR / "work_to_refs",
        STAGING_DIR / "work_author_inst",
        STAGING_DIR / "dim_author",
        STAGING_DIR / "dim_institution",
    ] if not p.exists()
]
if missing_dirs:
    raise FileNotFoundError(f"Missing staging folders: {', '.join(str(x) for x in missing_dirs)}")

# ---------- Connect ----------
DB_FILE.parent.mkdir(parents=True, exist_ok=True)
con = duckdb.connect(str(DB_FILE), config={"temp_directory": TMP_DIR})

# Pragmas
con.execute("PRAGMA enable_object_cache=false;")   # avoid caching big buffers while indexing
con.execute("PRAGMA threads=1;")  # keep low during CTAS to reduce peak RAM; can raise later
con.execute("PRAGMA preserve_insertion_order=false;")
con.execute("PRAGMA memory_limit='32GB';")
con.execute(f"PRAGMA temp_directory='{TMP_DIR}';")
con.execute("PRAGMA progress_bar_time=5;")
con.execute("PRAGMA disable_progress_bar;")

print("Connected to", DB_FILE.resolve())

# ---------- Schemas ----------
con.execute("CREATE SCHEMA IF NOT EXISTS core;")
con.execute("CREATE SCHEMA IF NOT EXISTS marts;")

# ---------- Build tables in one transaction ----------


con.execute("BEGIN;")

print("Loading core.work ...")
con.execute(f"""
CREATE OR REPLACE TABLE core.work AS
SELECT
  CAST(work_id AS BIGINT)                       AS work_id,
  CAST(publication_year AS INTEGER)             AS publication_year,
  CAST(subfield AS INTEGER)                     AS subfield,
  CAST(type_is_core AS BOOLEAN)                 AS type_is_core,
  CAST(type_is_proceedings AS BOOLEAN)          AS type_is_proceedings,
  CAST(open_access_is_oa AS BOOLEAN)            AS open_access_is_oa,
  CAST(open_access_oa_status AS VARCHAR)        AS open_access_oa_status,
  CAST(open_access_oa_status_is_gold AS BOOLEAN)  AS open_access_oa_status_is_gold,
  CAST(open_access_oa_status_is_closed AS BOOLEAN)  AS open_access_oa_status_is_closed,
  CAST(open_access_oa_status_is_bronze AS BOOLEAN)  AS open_access_oa_status_is_bronze,
  CAST(open_access_oa_status_is_green AS BOOLEAN)   AS open_access_oa_status_is_green,
  CAST(open_access_oa_status_is_hybrid AS BOOLEAN)  AS open_access_oa_status_is_hybrid,
  CAST(open_access_oa_status_is_diamond AS BOOLEAN) AS open_access_oa_status_is_diamond,
  CAST(open_access_any_repository_has_fulltext AS BOOLEAN) AS open_access_any_repository_has_fulltext
FROM read_parquet('{P_WORK}');
""")

print("Loading core.work_to_status ...")
con.execute(f"""
CREATE OR REPLACE TABLE core.work_to_status AS
SELECT
  CAST(work_id AS BIGINT)            AS work_id,
  CAST(publication_year AS INTEGER)  AS publication_year,
  CAST(is_included AS BOOLEAN)       AS is_included,
  CAST(is_english AS BOOLEAN)        AS is_english,
  CAST(has_authorship AS BOOLEAN)    AS has_authorship,
  CAST(has_subfield AS BOOLEAN)      AS has_subfield,
  CAST(has_referenced_works AS BOOLEAN) AS has_referenced_works,
  CAST(is_core_or_proceedings AS BOOLEAN) AS is_core_or_proceedings,
  CAST(is_not_retracted AS BOOLEAN)  AS is_not_retracted,
  CAST(is_not_paratext AS BOOLEAN)   AS is_not_paratext
FROM read_parquet('{P_WORK_TO_STATUS}');
""")

print("Loading core.refs ...")
con.execute(f"""
CREATE OR REPLACE TABLE core.refs AS
SELECT
  CAST(work_id AS BIGINT) AS citing_id,
  CAST(ref_id  AS BIGINT) AS cited_id
FROM read_parquet('{P_WORK_TO_REFS}');
""")

print("Loading core.work_author_inst ...")
con.execute(f"""
CREATE OR REPLACE TABLE core.work_author_inst AS
SELECT
  CAST(work_id AS BIGINT)           AS work_id,
  CAST(author_id AS BIGINT)         AS author_id,
  CAST(institution_id AS BIGINT)    AS institution_id,
  CAST(is_first_author AS BOOLEAN)  AS is_first_author,
  CAST(is_last_author  AS BOOLEAN)  AS is_last_author
FROM read_parquet('{P_WORK_AUTHOR_INST}');
""")

print("Building core.authors ...")
con.execute("""
CREATE OR REPLACE TABLE core.authors AS
SELECT DISTINCT
  work_id,
  author_id
FROM core.work_author_inst;
""")

print("Loading core.dim_author ...")
con.execute(f"""
CREATE OR REPLACE TABLE core.dim_author AS
SELECT
  CAST(author_id AS BIGINT) AS author_id,
  CAST(orcid AS VARCHAR)    AS orcid,
  CAST(name AS VARCHAR)     AS name
FROM read_parquet('{P_DIM_AUTHOR}');
""")

print("Building core.dim_country (country_id -> country_code) ...")
con.execute(f"""
CREATE OR REPLACE TABLE core.dim_country AS
WITH codes AS (
  SELECT DISTINCT CAST(country_code AS VARCHAR) AS country_code
  FROM read_parquet('{P_DIM_INSTITUTION}')
  WHERE country_code IS NOT NULL
),
ranked AS (
  SELECT
    DENSE_RANK() OVER (ORDER BY country_code) AS country_id,
    country_code
  FROM codes
)
SELECT country_id::INTEGER AS country_id, country_code
FROM ranked;
""")

print("Loading core.dim_institution (with country_id) ...")
con.execute(f"""
CREATE OR REPLACE TABLE core.dim_institution AS
WITH base AS (
  SELECT
    CAST(institution_id AS BIGINT) AS institution_id,
    CAST(ror AS VARCHAR)           AS ror,
    CAST(name AS VARCHAR)          AS name,
    CAST(type AS VARCHAR)          AS type,
    CAST(country_code AS VARCHAR)  AS country_code
  FROM read_parquet('{P_DIM_INSTITUTION}')
),
joined AS (
  SELECT
    b.institution_id,
    b.ror,
    b.name,
    b.type,
    dc.country_id::INTEGER AS country_id
  FROM base b
  LEFT JOIN core.dim_country dc
    ON b.country_code = dc.country_code
)
SELECT * FROM joined;
""")

# End the big CTAS transaction before indexing
con.execute("COMMIT;")
con.execute("CHECKPOINT;")

# ---------- Indexes (staged with spill) ----------
print("Creating indexes (staged with spill + checkpoints) ...")

def mkindex(sql: str):
    print (sql)
    con.execute(sql)        # autocommit mode

con.execute("PRAGMA memory_limit='32GB';")

mkindex("CREATE INDEX IF NOT EXISTS idx_dim_country_pk       ON core.dim_country(country_id);")

# Light to medium
mkindex("CREATE INDEX IF NOT EXISTS idx_work_pk              ON core.work(work_id);")
mkindex("CREATE INDEX IF NOT EXISTS idx_work_year            ON core.work(publication_year);")
mkindex("CREATE INDEX IF NOT EXISTS idx_work_subfield        ON core.work(subfield);")
mkindex("CREATE INDEX IF NOT EXISTS idx_work_typeproc        ON core.work(type_is_proceedings);")

mkindex("CREATE INDEX IF NOT EXISTS idx_wstatus_work         ON core.work_to_status(work_id);")

mkindex("CREATE INDEX IF NOT EXISTS idx_auth_work            ON core.authors(work_id);")
mkindex("CREATE INDEX IF NOT EXISTS idx_auth_author          ON core.authors(author_id);")

mkindex("CREATE INDEX IF NOT EXISTS idx_dim_author_pk        ON core.dim_author(author_id);")
mkindex("CREATE INDEX IF NOT EXISTS idx_dim_country_code     ON core.dim_country(country_code);")
mkindex("CREATE INDEX IF NOT EXISTS idx_dim_inst_pk          ON core.dim_institution(institution_id);")
mkindex("CREATE INDEX IF NOT EXISTS idx_dim_inst_country     ON core.dim_institution(country_id);")

# Heavier last (large table)
mkindex("CREATE INDEX IF NOT EXISTS idx_wai_work             ON core.work_author_inst(work_id);")
mkindex("CREATE INDEX IF NOT EXISTS idx_wai_author           ON core.work_author_inst(author_id);")
mkindex("CREATE INDEX IF NOT EXISTS idx_wai_inst             ON core.work_author_inst(institution_id);")



# ---------- Analyze ----------
print("ANALYZE core ...")
con.execute("ANALYZE;")

con.close()
print(f"✓ Built data warehouse: {DB_FILE.resolve()}")