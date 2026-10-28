'''
Download the full OpenAlex snapshot (all entities) from the public S3 bucket into SNAPSHOT_DIR.
The download is resumable: re-running the script skips files that are already present.
Pass test=True to skip large files (useful to try the pipeline without downloading ~400 GB).
'''

from modules.openalex_snapshot.OpenAlexSnapshot import OpenAlexSnapshot

SNAPSHOT_DIR = 'snapshot_downloaded_2025_09_03'

snap = OpenAlexSnapshot(local_dir=SNAPSHOT_DIR)
snap.download(test=False)
