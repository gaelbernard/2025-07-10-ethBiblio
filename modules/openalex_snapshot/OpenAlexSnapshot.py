import os
import json
import logging
import gzip
import shutil
import subprocess
import orjson
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from tqdm import tqdm

# Detect pigz at import time
PIGZ_CMD = shutil.which("pigz")
if PIGZ_CMD:
    logging.info(f"Using pigz for decompression: {PIGZ_CMD}")
else:
    logging.warning("pigz not found; falling back to gzip.open()")

def _local_file_path(base_dir: str, url: str) -> str:
    """Turn an S3 URI or local data path into a local filesystem path under base_dir."""
    if url.startswith("s3://"):
        rel = url.split('/', 4)[4]
    else:
        rel = url.replace('data/', '', 1)
    return os.path.join(base_dir, rel)

def _process_file_stream(local_path: str):
    """
    Yield each JSON object from a .gz JSON-lines file.
    Uses pigz -dc if available, else gzip.open.
    """
    if PIGZ_CMD:
        proc = subprocess.Popen(
            [PIGZ_CMD, "-dc", local_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        assert proc.stdout
        for line in proc.stdout:
            yield orjson.loads(line)
        ret = proc.wait()
        if ret != 0:
            logging.error(f"pigz failed ({ret}), falling back to gzip for {local_path}")
            # fallback once
            with gzip.open(local_path, "rb") as fin:
                for line in fin:
                    yield orjson.loads(line)
    else:
        with gzip.open(local_path, "rb") as fin:
            for line in fin:
                yield orjson.loads(line)

class OpenAlexSnapshot:
    """
    Single-threaded loader for OpenAlex .gz JSON-lines snapshots,
    using pigz if installed for fastest in-process decompression.
    """

    MAX_RECORDS_TEST = 20_000
    ENTITIES = [
        'authors', 'concepts', 'domains', 'fields', 'funders',
        'institutions', 'publishers', 'sources', 'subfields',
        'topics', 'works'
    ]

    def __init__(self, local_dir: str = 'openalex_snapshot'):
        self.local_dir = local_dir
        os.makedirs(self.local_dir, exist_ok=True)

        log_file = os.path.join(self.local_dir, 'snapshot.log')
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s %(levelname)s %(message)s'
        )
        logging.info('OpenAlexSnapshot initialized')

        cfg = Config(signature_version=UNSIGNED, retries={'max_attempts': 3})
        self.s3 = boto3.client('s3', config=cfg)
        self.bucket = 'openalex'
        self.manifests = None

    def download(self, test: bool = False):
        self.manifests = {}
        for entity in self.ENTITIES:
            ent_dir = os.path.join(self.local_dir, entity)
            os.makedirs(ent_dir, exist_ok=True)
            manifest_s3_key = f'data/{entity}/manifest'
            manifest_local = os.path.join(ent_dir, 'manifest.json')

            if not os.path.exists(manifest_local):
                logging.info(f'Downloading manifest for {entity}')
                self.s3.download_file(self.bucket, manifest_s3_key, manifest_local)

            with open(manifest_local, 'r', encoding='utf-8') as f:
                entries = json.load(f).get('entries', [])
            self.manifests[entity] = entries

            if test:
                entries = [
                    e for e in entries
                    if e.get('meta', {}).get('record_count', 0) <= self.MAX_RECORDS_TEST
                ]

            logging.info(f'Downloading {len(entries)} blobs for {entity} (test={test})')
            for entry in tqdm(entries, desc=f"Downloading {entity}"):
                url = entry['url'].replace('s3://openalex/', '')
                local = _local_file_path(self.local_dir, entry['url'])
                tmp = local + '.tmp'
                os.makedirs(os.path.dirname(local), exist_ok=True)
                if not os.path.exists(local):
                    try:
                        self.s3.download_file(self.bucket, url, tmp)
                        os.replace(tmp, local)
                    except Exception as e:
                        logging.error(f'Error downloading {url}: {e}')

    def load_manifests(self):
        self.manifests = {}
        for entity in self.ENTITIES:
            manifest_path = os.path.join(self.local_dir, entity, 'manifest.json')
            if not os.path.exists(manifest_path):
                raise FileNotFoundError(f"Missing manifest for {entity}")
            with open(manifest_path, 'r', encoding='utf-8') as f:
                self.manifests[entity] = json.load(f).get('entries', [])
            logging.info(f"Loaded manifest for {entity}: {len(self.manifests[entity])} entries")

    def __iterate(self, entity: str, test: bool = False, show_progress: bool = True):

        if self.manifests is None:
            self.load_manifests()
        if entity not in self.manifests:
            raise RuntimeError(f"Manifest not loaded for '{entity}'")

        # Filter entries for test mode and existence
        entries = self.manifests[entity]
        if test:
            entries = [
                e for e in entries
                if os.path.exists(_local_file_path(self.local_dir, e['url']))
                and e.get('meta', {}).get('record_count', 0) <= self.MAX_RECORDS_TEST
            ]
        else:
            # raise an error if one of the files is missing
            for entry in entries:
                local = _local_file_path(self.local_dir, entry['url'])
                if not os.path.exists(local):
                    raise FileNotFoundError(f"Missing file for {entity}: {local}")

        return entries

    def iterate(self, entity: str, test: bool = False, show_progress: bool = True):
        """
        Yield every record for `entity`, streaming one file at a time.
        If `test=True`, skips files with too many records or missing locally.
        If `show_progress=True`, displays a single progress bar over files.
        """

        entries = self.__iterate(entity, test=test, show_progress=show_progress)

        # Wrap in tqdm only if requested
        file_iter = tqdm(entries, desc=f"Iterating {entity}", ncols=80) if show_progress else entries

        for entry in file_iter:
            local = _local_file_path(self.local_dir, entry['url'])
            for rec in _process_file_stream(local):
                yield rec

    @staticmethod
    def extract_work_flags(work: dict) -> dict:
        """
        Take a work exactly as provided from the snapshot and returns a boolean with various conditions.
        If one of those conditions is false, the work will not be included in the analysis

        Flags:
          - is_english
          - has_authorship
          - has_subfield
          - has_referenced_works
          - is_core_or_proceedings
          - is_not_retracted
          - is_not_paratext
        """
        # language
        language = (work.get("language") or "").strip().lower()

        # authorships present?
        authorships = work.get("authorships") or []
        has_authorship = len(authorships) > 0

        # referenced works present?
        referenced_works = work.get("referenced_works") or []
        has_referenced_works = len(referenced_works) > 0

        # proceedings / core
        is_proceedings = (work.get("type_crossref") == "proceedings-article")
        is_core = (((work.get("primary_location") or {}).get("source") or {}).get("is_core", False))

        # subfield detectable from primary_topic.subfield.id (e.g., ".../1234")
        subfield_id = (((work.get("primary_topic") or {}).get("subfield") or {}).get("id"))
        tail = str(subfield_id).rpartition("/")[2]
        has_subfield = tail.isdigit()

        return {
            "flag_is_english": language == "en",
            "flag_has_authorship": has_authorship,
            "flag_has_subfield": has_subfield,
            "flag_has_referenced_works": has_referenced_works,
            "flag_is_core_or_proceedings": bool(is_core or is_proceedings),
            "flag_is_not_retracted": not bool(work.get("is_retracted", False)),
            "flag_is_not_paratext": not bool(work.get("is_paratext", False)),
        }

    def get_total_size_in_gb(self) -> float:
        total = 0
        for entries in self.manifests.values():
            for e in entries:
                total += e.get('meta', {}).get('content_length', 0)
        return total / (1024 ** 3)


if __name__ == '__main__':
    snap = OpenAlexSnapshot(local_dir='openalex_snapshot')
    snap.load_manifests()

    # Example: count works in test mode
    count = 0
    for rec in snap.iterate('works', test=True):
        count += 1
    print(f"Iterated {count} work records")