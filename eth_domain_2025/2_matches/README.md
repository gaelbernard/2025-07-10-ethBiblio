# 2_matches

Match the publication list provided by each institution's library to works of the OpenAlex snapshot. Run the scripts from this folder, in order, for the six institutions (`ethz`, `epfl`, `psi`, `wsl`, `empa`, `eawag`).

The library lists are **not included** in this repository. Each list goes in `{institution}/input/` (see the file names in `01_prepare_input.py`) and must contain at least: `title`, `internal_id`, `dois`, `openalex_id`, `year`, `publication_type`, `authors_all`, `journal`.

1. [`01_prepare_input.py`](01_prepare_input.py) standardises each library export (column names, DOI prefixes, list fields, OpenAlex IDs) into `{institution}/input/qpub.parquet`, adding `original_line_number` to keep track of the original order.
2. [`02_discover_matches.py`](02_discover_matches.py) compares each list with the Parquet works from `1_data` and keeps every candidate that matches on at least one of: OpenAlex ID, DOI, or title (SimHash). Candidates are written to `{institution}/final_match/`.
3. [`03_pick_best_matches.py`](03_pick_best_matches.py) scores the candidates (title, journal and author similarity, rare title, same year, in-scope flags), removes retracted works and low-confidence candidates, and writes:
   - `{institution}/final_match/final_list_all_matches.parquet`: all remaining candidates, plus `not_found` rows, in the order of the input list;
   - `{institution}/final_match/final_list_top_match.parquet`: the best match per input publication.
4. [`ethdomain/merge.py`](ethdomain/merge.py) (run from `ethdomain/`) concatenates the six top-match files into `ethdomain/final_match/final_list_top_match.parquet` for the ETH Domain report.

A candidate found only by title is kept if the title is similar (≥ 0.7) **and** either the title occurs at most once in the snapshot or both journal and authors are similar (≥ 0.7). Candidates found by OpenAlex ID or DOI are always kept. When several candidates remain for one publication, the one with the highest priority score is selected:

> priority = 4 × (work is in scope) + title similarity + journal similarity + authors similarity + (OpenAlex ID match) + (DOI match) + (same year)

Title occurrence counts are computed in one pass over the snapshot for the titles of all six lists and cached next to the Parquet folder (`snapshot_in_parquets_2025_10_15_title_counts.pickle`).

The matching logic itself lives in [`modules/match`](../../modules/match).
