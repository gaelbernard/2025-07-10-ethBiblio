#!/usr/bin/env python3
import lmdb
from pathlib import Path
import tqdm
import pandas as pd
import json
from collections import OrderedDict
import duckdb

from utils import DB_FILE

DB_PATH = Path(DB_FILE)
author_map = lmdb.open(str(Path("gender/kv/author_map.lmdb")), readonly=True, lock=False)
name_cache = lmdb.open(str(Path("gender/kv/name_cache.lmdb")), readonly=True, lock=False)
count, missing = 0, 0

output = []
with author_map.begin() as author_map_, name_cache.begin() as name_cache_:
    cursor = author_map_.cursor()
    for author_id_bytes, firstname_bytes in tqdm.tqdm(cursor, desc="Iterating authors", total=author_map_.stat()['entries']):
        count += 1

        # Decode both key and value
        author_id = author_id_bytes.decode("utf-8")
        firstname = firstname_bytes.decode("utf-8")

        # Retrieve gender from second DB
        gender_bytes = name_cache_.get(firstname.encode("utf-8"))
        if gender_bytes is None:
            # Not in the cache: the author ends up with an unknown gender (no row in augmented.author_gender)
            missing += 1
            continue
        gender = json.loads(gender_bytes.decode("utf-8"))

        gender['author_id'] = author_id
        if 'initial_only' not in gender:
            gender['initial_only'] = False
        gender['initial_only'] = bool(gender['initial_only'])

        # Reorder so author_id is first
        gender = OrderedDict([('author_id', author_id)] + [(k, v) for k, v in gender.items() if k != 'author_id'])

        output.append(gender)


output = pd.DataFrame(output)
print(f"{count:,} authors read, {missing:,} without a cached name")

con = duckdb.connect(str(DB_PATH))
con.execute("CREATE SCHEMA IF NOT EXISTS augmented;")

con.register("output", output)
con.execute("""
    CREATE OR REPLACE TABLE augmented.author_gender AS
    SELECT
        CAST(o.author_id AS BIGINT) AS author_id,
        o.name,
        o.gender,
        o.probability,
        o."count" AS count,
        o.initial_only
    FROM output AS o
""")
con.execute("""
    CREATE INDEX IF NOT EXISTS idx_author_gender_author_id
    ON augmented.author_gender(author_id)
""")

con.execute("CHECKPOINT;")
con.close()



