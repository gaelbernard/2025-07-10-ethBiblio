from pathlib import Path

# Paths are relative to this folder (eth_domain_2025/3_dwh/1_build), from which all build scripts are run.
DB_FILE      = Path("../v03_eth_domain.duckdb")
SNAPSHOT_DIR = Path("../../1_data/snapshot_downloaded_2025_09_03")
DUCKDB_TMP   = Path("duckdb_tmp")  # spill directory for large DuckDB operations
