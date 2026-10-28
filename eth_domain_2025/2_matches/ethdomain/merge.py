'''
Concatenate the six institutional top-match files into a single ETH Domain list
(ethdomain/final_match/final_list_top_match.parquet). Run from this folder after 03_pick_best_matches.py.
'''
import pandas as pd
from pathlib import Path

# ---------- Config ----------
MATCH_ROOT = Path("../")
# The offset keeps original_line_number unique across institutions
SOURCES = [
    ("epfl",  1_000_000),
    ("ethz",  2_000_000),
    ("psi",   3_000_000),
    ("empa",  4_000_000),
    ("wsl",   5_000_000),
    ("eawag", 6_000_000),
]

OUTPUT_PATH = MATCH_ROOT / "ethdomain" / "final_match" / "final_list_top_match.parquet"
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# ---------- Collect all sources ----------
frames = []
for inst, offset in SOURCES:
    p = MATCH_ROOT / inst / "final_match" / "final_list_top_match.parquet"
    if not p.exists():
        print(f"⚠️  Skipping {inst}: file not found at {p}")
        continue

    df = pd.read_parquet(p)
    df = df.rename(columns={
        "qpub_original_line_number": "original_line_number"
    })[["original_line_number", "tpub_id"]].copy()

    df["original_line_number"] = (
        df["original_line_number"].astype("Int64").astype("Float64") + offset
    ).astype("Int64")
    df["tpub_id"] = df["tpub_id"].astype("Int64")

    df["report_source"] = inst
    frames.append(df)

# ---------- Combine and save ----------
if frames:
    df_all = pd.concat(frames, ignore_index=True)
    assert df_all.columns.duplicated().sum() == 0, f"Duplicate columns: {list(df_all.columns[df_all.columns.duplicated()])}"

    df_all.to_parquet(OUTPUT_PATH, index=False)
    print(f"✓ Wrote {len(df_all):,} rows to {OUTPUT_PATH}")
else:
    print("⚠️  No input parquet files found — nothing written.")
