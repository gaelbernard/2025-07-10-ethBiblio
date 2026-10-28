#!/usr/bin/env python3
import os
import time, random, json
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import duckdb
import requests
import lmdb
import tqdm
from dotenv import load_dotenv

# ---------- Config ----------
from utils import DB_FILE

load_dotenv()  # reads the .env file at the repository root (see .env.example)

DB_PATH = Path(DB_FILE)
API_KEY   = os.environ.get("GENDERIZE_API_KEY")
if not API_KEY:
    raise RuntimeError("GENDERIZE_API_KEY is not set. Copy .env.example to .env and add your Genderize key.")
BASE_URL  = "https://api.genderize.io"
STORE_DIR = Path("gender/kv")

BATCH_NAMES = 10               # names per API call
MAP_SIZE    = 2_000_000_000    # 2 GB LMDB files (adjust if needed)
MIN_INTERVAL = 0.2             # tqdm refresh rate

STORE_DIR.mkdir(parents=True, exist_ok=True)

# ---------- Helpers ----------
def norm_name(n: str) -> str:
    """Normalize to first token before space or hyphen, lowercase."""
    if not n:
        return ""
    n = n.strip().lower()
    return n.split()[0].split("-")[0]

def is_initial_only(s: str) -> bool:
    """True if first token is a single-letter initial."""
    if not s:
        return False
    t = s.replace(".", " ").replace("-", " ").strip().split()
    return bool(t) and len(t[0]) == 1

# ---------- LMDB helpers ----------
def name_cache_get(env_name: lmdb.Environment, qname: str) -> Optional[Dict]:
    with env_name.begin(write=False) as txn:
        v = txn.get(qname.encode("utf-8"))
    return json.loads(v.decode("utf-8")) if v else None

def name_cache_put_many(env_name: lmdb.Environment, pairs: List[Tuple[str, Dict]]) -> None:
    dedup: Dict[str, Dict] = {}
    for k, v in pairs:
        dedup[k] = v
    if not dedup:
        return
    with env_name.begin(write=True) as txn:
        for q, resp in dedup.items():
            txn.put(q.encode("utf-8"), json.dumps(resp, separators=(",", ":")).encode("utf-8"))

def author_map_get(env_auth: lmdb.Environment, author_id: int) -> Optional[str]:
    key = str(int(author_id)).encode("utf-8")
    with env_auth.begin(write=False) as txn:
        v = txn.get(key)
    return v.decode("utf-8") if v else None

def author_map_put_many(env_auth: lmdb.Environment, rows: List[Tuple[int, str]]) -> None:
    if not rows:
        return
    with env_auth.begin(write=True) as txn:
        for aid, q in rows:
            txn.put(str(int(aid)).encode("utf-8"), q.encode("utf-8"))

# ---------- API ----------
_session = requests.Session()

def call_genderize_batch(names: List[str], max_retries=5, backoff=1.6) -> List[Dict]:
    """Call Genderize for a batch of names with retries."""
    if not names:
        return []
    for i in range(max_retries):
        try:
            params = [("apikey", API_KEY)] + [("name[]", n) for n in names]
            r = _session.get(BASE_URL, params=params, timeout=20)
            if r.ok:
                data = r.json()
                return data if isinstance(data, list) else [data]
        except requests.RequestException:
            pass
        time.sleep((backoff ** i) + random.uniform(0, 0.3))
    return [{"name": n, "gender": None, "probability": None, "count": 0, "error": True} for n in names]

# ---------- Main reusable processor ----------
def process_authors_from_query(con: duckdb.DuckDBPyConnection, query: str, label: str) -> None:
    """Process a set of authors from any SQL query returning (author_id, name)."""
    print(f"\n=== Processing authors: {label} ===")
    authors = con.execute(query).df()

    if authors.empty:
        print("No authors found in query. Nothing to do.")
        return

    # Open LMDB envs (persistent caches)
    env_name = lmdb.open(str(STORE_DIR / "name_cache.lmdb"), map_size=MAP_SIZE, subdir=True, lock=True, max_dbs=1)
    env_auth = lmdb.open(str(STORE_DIR / "author_map.lmdb"), map_size=MAP_SIZE, subdir=True, lock=True, max_dbs=1)

    # Filter already processed authors
    mask_seen = authors["author_id"].map(lambda x: author_map_get(env_auth, int(x)) is not None)
    authors = authors[~mask_seen]

    if authors.empty:
        print("All authors already processed. Nothing to do.")
        tqdm.tqdm(total=0, desc="Authors").close()
        return

    # Prepare normalized names and flags
    authors = authors.assign(
        query_name=authors["name"].astype(str).map(norm_name),
        initial_only=lambda df: df["query_name"].map(is_initial_only),
    )

    pbar = tqdm.tqdm(total=len(authors), desc="Authors", mininterval=MIN_INTERVAL, smoothing=0.1)

    # ---------- Step 1: Initial-only ----------
    df_init = authors[authors["initial_only"]]
    if not df_init.empty:
        name_upserts, auth_upserts = [], []
        for r in df_init.itertuples(index=False):
            resp = {"name": r.query_name, "gender": None, "probability": None, "count": 0, "initial_only": True}
            name_upserts.append((r.query_name, resp))
            auth_upserts.append((int(r.author_id), r.query_name))
        name_cache_put_many(env_name, name_upserts)
        author_map_put_many(env_auth, auth_upserts)
        pbar.update(len(df_init))

    # ---------- Step 2: Non-initial ----------
    df_non = authors[~authors["initial_only"]]
    if not df_non.empty:
        # Separate cache hits and misses
        df_non = df_non.assign(_cached=df_non["query_name"].map(lambda q: name_cache_get(env_name, q)))
        df_hits = df_non[~df_non["_cached"].isna()]
        df_miss = df_non[df_non["_cached"].isna()]

        if not df_hits.empty:
            author_map_put_many(env_auth, [(int(r.author_id), r.query_name) for r in df_hits.itertuples(index=False)])
            pbar.update(len(df_hits))

        # Misses: batch API calls
        if not df_miss.empty:
            miss_names = df_miss["query_name"].tolist()
            name_counts = df_miss["query_name"].value_counts().to_dict()

            # preserve order, remove duplicates
            seen, unique_names = set(), []
            for n in miss_names:
                if n not in seen:
                    seen.add(n)
                    unique_names.append(n)

            for i in range(0, len(unique_names), BATCH_NAMES):
                batch = unique_names[i:i + BATCH_NAMES]
                results = call_genderize_batch(batch)
                pairs = [(r.get("name"), {
                    "name": r.get("name"),
                    "gender": r.get("gender"),
                    "probability": r.get("probability"),
                    "count": r.get("count"),
                }) for r in results]
                name_cache_put_many(env_name, pairs)

                covered_authors = sum(name_counts.get(n, 0) for n in batch)
                if covered_authors:
                    pbar.update(covered_authors)

            author_map_put_many(env_auth, [(int(r.author_id), r.query_name) for r in df_miss.itertuples(index=False)])

    pbar.close()
    print(f"Finished processing: {label}\n")

# ---------- Main ----------
if __name__ == "__main__":
    con = duckdb.connect(str(DB_PATH))

    # 1) Authors of the works listed by the institutions (processed first)
    query_reports = """
    SELECT DISTINCT a.author_id, d.name
    FROM reports.report_membership rm
    JOIN core.work_author_inst a ON rm.work_id = a.work_id
    JOIN core.dim_author d USING(author_id)
    WHERE rm.work_id IS NOT NULL
    """
    process_authors_from_query(con, query_reports, label="report_membership works")

    # 2) All other authors of works published 2014–2023, in random order (resumable via the LMDB cache)
    query_random = """
    WITH random_works AS (
        SELECT work_id
        FROM core.work
        WHERE publication_year BETWEEN 2014 AND 2023
        ORDER BY RANDOM()
    )
    SELECT DISTINCT a.author_id, d.name
    FROM random_works rw
    JOIN core.work_author_inst a ON rw.work_id = a.work_id
    JOIN core.dim_author d USING(author_id);
    """
    process_authors_from_query(con, query_random, label="random works 2014–2023")