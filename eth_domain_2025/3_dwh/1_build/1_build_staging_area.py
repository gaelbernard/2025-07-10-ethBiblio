# python >=3.10
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from modules.openalex_snapshot.OpenAlexSnapshot import OpenAlexSnapshot
import os
import json
from utils import SNAPSHOT_DIR
# ---------- Config ----------
SNAP_DIR = str(SNAPSHOT_DIR)
OUT_DIR = Path("1_staging_area")
BUF = 100_000  # flush when any table exceeds this many rows
test_mode = False

if os.path.exists(OUT_DIR) and test_mode is False:
    raise ValueError('Directory already exists, remove it to run the script. ')

OUT_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_OPTS = dict(
    compression="zstd",
    compression_level=3,
    use_dictionary=True,
    write_statistics=True,
)

def atomic_write_parquet(rows: list[dict], out_path: Path):
    if not rows:
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    pq.write_table(table, tmp, **PARQUET_OPTS)
    tmp.replace(out_path)  # atomic on same filesystem

def flush_table(db: dict[str, list[dict]], table: str, counters: dict[str, int]):
    """Write one table if it has rows; clear it. File name increments per table."""
    rows = db[table]
    if not rows:
        return
    part = counters.get(table, 0)
    out_path = OUT_DIR / table / f"part-{part:06d}.parquet"
    atomic_write_parquet(rows, out_path)
    db[table].clear()
    counters[table] = part + 1

def flush_if_needed(db: dict[str, list[dict]], counters: dict[str, int], threshold: int = BUF, force: bool = False):
    """Flush any table whose buffer length >= threshold."""
    for table, rows in db.items():
        if len(rows) >= threshold or force:
            flush_table(db, table, counters)

# ---------- Init ----------
snap = OpenAlexSnapshot(local_dir=SNAP_DIR)
snap.load_manifests()

DB = {
    "dim_author": [],
    "dim_institution": [],
    "work_to_status": [],
    "work_author_inst": [],
    "work": [],
    "work_to_refs": [],
}
COUNTERS = {}  # per-table part counters

print ('\n\n\n---------- WORKS ----------')
is_first = True
i = 0
for work in snap.iterate("works", test=test_mode, show_progress=True):
    i += 1
    if is_first:
        print(json.dumps(work, indent=4))
        is_first = False

    # Retrieve and clean data
    work_id = int(work["id"][22:])
    publication_year = work.get("publication_year")
    language = (work.get("language") or "").lower()
    authorships = work.get("authorships") or []

    # --- weights setup ---
    n_authors = max(1, len(authorships))

    unique_inst_ids = set()
    for a in authorships:
        for inst in (a.get("institutions") or []):
            if inst.get("id"):
                unique_inst_ids.add(int(inst["id"][22:]))
    n_insts = max(1, len(unique_inst_ids))

    work_author_inst = []
    for author in authorships:
        insts = author.get('institutions') or []

        for inst in insts:
            work_author_inst.append({
                'work_id': work_id,
                'author_id': int(author['author']['id'][22:]),
                'is_first_author': author.get('author_position') == 'first',
                'is_last_author': author.get('author_position') == 'last',
                'institution_id': int(inst.get('id')[22:]),
            })

    referenced_works = [int(x[22:]) for x in (work.get('referenced_works') or [])]
    is_core = (((work.get("primary_location") or {}).get("source") or {}).get("is_core", False))
    is_proceedings = (work.get("type_crossref") == "proceedings-article")

    # get subfield
    v = ((work.get("primary_topic") or {}).get("subfield") or {}).get("id")
    tail = str(v).rpartition('/')[2]
    subfield = int(tail) if tail.isdigit() else None

    conditions = {
        'is_english': language == "en",
        'has_authorship': len(work_author_inst)>0,
        'has_subfield': subfield is not None,
        'has_referenced_works': len(referenced_works)>0,
        'is_core_or_proceedings': is_core or is_proceedings,
        'is_not_retracted': not work['is_retracted'],
        'is_not_paratext': not work['is_paratext'],
    }
    is_included = all(conditions.values())

    # Save the status (even if the publication is not included
    DB['work_to_status'].append({'work_id': work_id, 'publication_year':publication_year, 'is_included':is_included, **conditions})
    if i % 1000 == 0: # Avoid checking if flush is needed at every records (remaining rows are flushed after the loop)
        flush_if_needed(DB, COUNTERS, BUF)

    # This is a publication we consider in the scope, let's save it
    if is_included:
        DB['work_author_inst'].extend(work_author_inst)
        oa = work.get("open_access") or {}

        DB["work"].append({
            "work_id": work_id,
            "publication_year": publication_year,
            "subfield": subfield,
            "type_is_core": is_core,
            "type_is_proceedings": is_proceedings,
            "open_access_is_oa": oa.get("is_oa"),
            "open_access_oa_status": oa.get("oa_status"),
            "open_access_oa_status_is_gold": oa.get("oa_status") == 'gold',
            "open_access_oa_status_is_closed": oa.get("oa_status") == 'closed',
            "open_access_oa_status_is_bronze": oa.get("oa_status") == 'bronze',
            "open_access_oa_status_is_green": oa.get("oa_status") == 'green',
            "open_access_oa_status_is_hybrid": oa.get("oa_status") == 'hybrid',
            "open_access_oa_status_is_diamond": oa.get("oa_status") == 'diamond',
            "open_access_any_repository_has_fulltext": oa.get("any_repository_has_fulltext"),
        })
        # Citation edges (src -> dst)
        for ref_id in referenced_works:
            DB["work_to_refs"].append({"work_id": work_id, "ref_id": ref_id})

        flush_if_needed(DB, COUNTERS, BUF)

flush_if_needed(DB, COUNTERS, BUF, force=True)



print ('\n\n\n---------- INSTITUTIONS ----------')
is_first = True
for inst in snap.iterate("institutions", test=test_mode, show_progress=True):

    if is_first:
        print(json.dumps(inst, indent=4))
        is_first = False

    DB["dim_institution"].append({
        "institution_id": int(inst["id"][22:]),
        "ror": inst["ror"],
        "name": inst['display_name'],
        "type": inst['type'],
        "country_code": inst['geo']['country_code'],
    })
    flush_if_needed(DB, COUNTERS, BUF)
flush_if_needed(DB, COUNTERS, BUF, force=True)


print ('\n\n\n---------- AUTHORS ----------')
is_first = True
for author in snap.iterate("authors", test=test_mode, show_progress=True):
    if is_first:
        print(json.dumps(author, indent=4))
        is_first = False

    DB["dim_author"].append({
        "author_id": int(author["id"][22:]),
        "orcid": author["orcid"],
        "name": author['display_name'],
    })
    flush_if_needed(DB, COUNTERS, BUF)
flush_if_needed(DB, COUNTERS, BUF, force=True)

print(f"Done.")