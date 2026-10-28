# OpenAlex Snapshot

Download the [OpenAlex snapshot](https://docs.openalex.org/download-all-data/openalex-snapshot) and stream its records.

The snapshot has one folder per entity (authors, concepts, domains, fields, funders, institutions, publishers, sources, subfields, topics, works). Each folder contains gzip-compressed JSON Lines files with one record per line, and a manifest listing the files.

## Download

The snapshot is downloaded anonymously from the public `openalex` S3 bucket. It takes several hours (about 421 GB in September 2025). The download is resumable: running it again skips the files already present.

```python
from modules.openalex_snapshot.OpenAlexSnapshot import OpenAlexSnapshot

snapshot = OpenAlexSnapshot('path/to/snapshot')
snapshot.download()
```

## Iterate

`iterate` yields every record of an entity as a Python dictionary, one file at a time. It raises `FileNotFoundError` if a file listed in the manifest is missing.

```python
snap = OpenAlexSnapshot('path/to/snapshot')
for work in snap.iterate('works'):
    ...
```

If [pigz](https://zlib.net/pigz/) is installed, it is used automatically for faster decompression.

`OpenAlexSnapshot.extract_work_flags(work)` returns the scope conditions of a work (English, has authorship, has subfield, has references, core source or proceedings, not retracted, not paratext).

## Test mode

`download(test=True)` skips large files (download in 10 to 20 minutes), and `iterate(entity, test=True)` only reads the small files that are present locally. Use `test=True` for both to try the code without the full snapshot.
