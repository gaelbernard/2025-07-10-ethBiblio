import os
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
from modules.openalex_snapshot.OpenAlexSnapshot import OpenAlexSnapshot

class ParquetBatchProcessor:
    """
    Parquet batch processor for OpenAlex snapshots.
    During the matching process, it would be too slow to process the compressed
    JSON files directly, and we only need a subset of the fields. Hence, this class
    prepares the data in a more efficient format; i.e., in parquet format in batches (typically 50,000 records per file).
    """
    def __init__(self,
                 input_dir: str = "data/snapshot",
                 output_dir: str = "data/dwh_publications",
                 batch_size: int = 100_000,
                 start_year: int = 2013,
                 end_year: int = 2024):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.start_year = start_year
        self.end_year = end_year

        if os.path.exists(self.output_dir):
            raise FileExistsError(f"The folder {self.output_dir} already exists.")
        if not os.path.exists(self.input_dir):
            raise FileNotFoundError(f"The input directory {self.input_dir} does not exist. Have you downloaded the OpenAlex snapshot?")
        os.makedirs(self.output_dir)

    def flush_batch(self, records: list) -> None:
        """
        Write the current batch of records to a Parquet file and clear the buffer.
        """
        if not records:
            return
        table = pa.Table.from_pylist(records)
        filename = f"batch_{uuid.uuid4().hex}.parquet"
        out_path = os.path.join(self.output_dir, filename)
        pq.write_table(table, out_path)
        records.clear()

    def process(self) -> None:
        """
        Load the snapshot and iterate through works, filtering
        by publication year and extracting relevant fields.
        """
        # Load snapshot
        snap = OpenAlexSnapshot(local_dir=self.input_dir)
        snap.load_manifests()

        buffer: list = []
        # Iterate all works
        for work in snap.iterate("works", test=False, show_progress=True):
            pub_year = work.get("publication_year") or 0
            if pub_year < self.start_year or pub_year > self.end_year:
                continue

            # Extract ORCIDs
            orcids = [
                ((a.get('author') or {}).get('orcid') or '')
                .replace('https://orcid.org/', '')
                for a in work.get("authorships") or []
            ]
            orcids = [o for o in orcids if o]
            flags = OpenAlexSnapshot.extract_work_flags(work)

            # Build record
            record = {
                "id": int(work.get("id", "").replace("https://openalex.org/W", "")),
                "doi": (work.get("doi") or '').replace("https://doi.org/", "").strip(),
                "title": work.get("title", ""),
                "type": work.get("type", ""),
                "authors_orcid": orcids,
                "authors_fullname": [
                    ((a.get('author') or {}).get('display_name') or '')
                    for a in work.get("authorships") or []
                ],
                "institutions_ids": set([
                    int(i.get('id').replace('https://openalex.org/I', ''))
                    for a in work.get("authorships") or []
                    for i in a.get('institutions') or []
                ]),
                "type_crossref": work.get("type_crossref", ""),
                "publication_year": pub_year,
                "primary_location": ((work.get("primary_location", {}) or {}).get('source') or {}).get('display_name'),
                "best_oa_location": ((work.get("best_oa_location", {}) or {}).get('source') or {}).get('display_name'),
                "primary_location_is_core": ((work.get("primary_location", {}) or {}).get('source') or {}).get('is_core', False),
                "volume": work['biblio'].get('volume'),
                "issue": work['biblio'].get('issue'),
                "is_oa": work['open_access'].get('is_oa'),
                "oa_status": work['open_access'].get('oa_status'),
                "any_repository_has_fulltext": work['open_access'].get('any_repository_has_fulltext'),
                "cited_by_count": work.get("cited_by_count", 0),
                "cited_by_2yr": work.get("summary_stats", {}).get("2yr_cited_by_count", 0),
                "is_retracted": work.get("is_retracted", False),
            }
            record.update(flags)
            buffer.append(record)

            # Flush if batch size reached
            if len(buffer) >= self.batch_size:
                self.flush_batch(buffer)

        # Flush remaining
        self.flush_batch(buffer)
        print(f"Finished exporting to {self.output_dir}")


if __name__ == "__main__":
    processor = ParquetBatchProcessor()
    processor.process()