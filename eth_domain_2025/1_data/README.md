# 1_data

Download the OpenAlex snapshot and prepare it for matching. Run the scripts from this folder.

1. [`01_download_openalex.py`](01_download_openalex.py) downloads the full snapshot (all entities) from the public OpenAlex S3 bucket into `snapshot_downloaded_2025_09_03/`. It is about **421 GB** (newer snapshots are larger) and the download is resumable. See [`modules/openalex_snapshot`](../../modules/openalex_snapshot) for details.
2. [`02_prepare_parquet_publications.py`](02_prepare_parquet_publications.py) converts works published 2013–2024 into Parquet batches (`snapshot_in_parquets_2025_10_15/`) that keep only the fields needed for matching. Parquet is much faster to scan than the compressed JSON Lines of the snapshot.

The study uses the snapshot downloaded on **3 September 2025**. With a newer snapshot, the code may need adjustments if the OpenAlex format has changed.

The [`patent`](patent) folder documents the external patent dataset added to the data warehouse.
