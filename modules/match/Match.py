from modules.match.Metrics import *
from modules.match.PriorityScore import *
from colorama import init, Fore, Style
import os
import json
from pathlib import Path
from functools import reduce
import operator
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from concurrent.futures import ProcessPoolExecutor, as_completed
import copy
import tqdm


class MatchPublications:
    """
    Orchestrates matching query publications (qpub; provided by libraries) against
    target publication files (tpub; OpenAlex Snapshot), using a set of Boolean metrics.
    """

    def __init__(self, path_qpub: str, path_tpub_root: str, path_match: str, max_workers: int = None, verbose: bool = False):
        """
        Initialize the matcher with paths to query publications, target publications root,
        and output directory. Optionally limit parallel workers.
        :param path_qpub: Path to the query publications Parquet file.
        :param path_tpub_root: Root directory containing target publications Parquet files.
        :param path_match: Directory where match results will be saved.
        :param max_workers: Maximum number of parallel worker processes (default: os.cpu_count()).
        """
        self.path_qpub = path_qpub
        self.path_tpub_root = path_tpub_root
        self.path_match = path_match

        os.makedirs(self.path_match, exist_ok=True)
        os.makedirs(f'{self.path_match}/parquets/', exist_ok=True)

        # List of (full_path, basename) for each target Parquet
        self.list_path_tpub = list(self._iterate_tpub_paths())

        # Metrics to apply (with qpub already loaded)
        self.metrics: list[Metric] = []

        # Number of parallel worker processes (None => os.cpu_count())
        self.max_workers = max_workers

        self.verbose = verbose

    def _iterate_tpub_paths(self):
        assert os.path.exists(self.path_tpub_root), f'Target path {self.path_tpub_root} does not exist.'
        for fn in os.listdir(self.path_tpub_root):
            if fn.endswith('.parquet'):
                yield os.path.join(self.path_tpub_root, fn), Path(fn).stem

    def add_metrics(self, metric: Metric):
        """
        Add a Metric instance, and load the query data into it.
        """
        assert isinstance(metric, Metric), 'metric must be an instance of Metric.'
        metric.verbose = self.verbose
        metric.load_qpub(self.path_qpub)
        self.metrics.append(metric)

    def _read_rows(self, path: str, indices: np.ndarray) -> pd.DataFrame:
        """
        Read only the specified global row indices from a Parquet file.
        """
        if indices.size == 0:
            return pd.DataFrame()
        pf = pq.ParquetFile(path)
        sizes = np.array([pf.metadata.row_group(i).num_rows for i in range(pf.num_row_groups)])
        bounds = np.concatenate([[0], sizes.cumsum()])
        grp_ids = np.searchsorted(bounds[1:], indices)
        tables = []
        for g in np.unique(grp_ids):
            offs = indices[grp_ids == g] - bounds[g]
            tbl = pf.read_row_group(g).take(pa.array(offs))
            tables.append(tbl)
        return pa.concat_tables(tables).to_pandas()

    def _collect_top_matches(self,
                             path_tpub: str,
                             name_tpub: str,
                             threshold: int,
                             weights: dict,
                             metrics: list[Metric]) -> None:
        """
        Combine metric match matrices, apply per-metric weights & threshold,
        and write a detailed match file using the provided metrics list.
        """
        if weights is None:
            weights = {}
        for w in weights:
            assert w in [m.name for m in metrics], \
                f'Metric "{w}" not in provided metrics.'

        # Sum all metrics’ weighted boolean match matrices into a single total score matrix
        total = reduce(
            operator.add,
            (
                m.match_matrix.astype(int) * weights.get(m.name, getattr(m, 'weight', 1))
                for m in metrics
            ),
        )

        # Pick all pairs whose weighted score >= threshold
        i_t, i_q = (total >= threshold).nonzero()
        if i_t.size == 0:
            if self.verbose:
                print(f'No matches ≥ {threshold} for {name_tpub}.')
            return
        if i_t.size > 1_000_000:
            raise ValueError('Too many matches; consider raising the threshold.')

        # Build a DataFrame with all matches with three parts:
        # 1. qpub data (from self.path_qpub; prefixed with 'qpub_')
        # 2. tpub data (from path_tpub; prefixed with 'tpub_')
        # 3. per-metric flags (from metrics; prefixed with 'metric_')
        # So that the output DataFrame is ideal for further analysis.
        df_q = self._read_rows(self.path_qpub, i_q).add_prefix('qpub_')
        df_t = self._read_rows(path_tpub, i_t).add_prefix('tpub_')
        metrics = {m.name: m.match_matrix[i_t, i_q].A1 for m in metrics}
        df_m = pd.DataFrame(metrics).add_prefix('metric_')
        out = pd.concat([df_q, df_t, df_m], axis=1)

        # Consistent column types across batches, so that all match files can be concatenated later
        out = out.convert_dtypes()

        num_int_cols = ['qpub_openalex_id', 'qpub_year', 'qpub_dc.type.family.index']
        for c in num_int_cols:
            if c in out.columns:
                out[c] = pd.to_numeric(out[c], errors='coerce').astype('Int64')

        metric_cols = [c for c in out.columns if c.startswith('metric_')]
        if metric_cols:
            out[metric_cols] = out[metric_cols].astype('float32')

        str_cols = out.select_dtypes(include='string').columns
        out[str_cols] = out[str_cols].fillna('')

        out.to_parquet(f'{self.path_match}/parquets/{name_tpub}.parquet', index=False)
        if self.verbose:
            print(f'Collected {len(i_t)} matches from {name_tpub} (threshold={threshold}, weights={weights})')



    def _process_one(self,
                     path_tpub: str,
                     name_tpub: str,
                     threshold: int,
                     weights: dict) -> dict:
        """
        Process a single target publication file.
        We do it in parallel to speed up the matching process
        so that each parquet file is processed independently.
        """
        metrics_copy = [copy.deepcopy(m) for m in self.metrics]

        # run each metric on this chunk

        for m in metrics_copy:
            m.run(path_tpub, name_tpub)

        # collect matches using the copied metrics explicitly
        self._collect_top_matches(path_tpub, name_tpub, threshold, weights, metrics_copy)

        # return timings
        return {m.name: m.apply_time for m in metrics_copy}

    def run(self, threshold: int = 2, weights: dict = None) -> None:
        """
        Execute all metrics on each target file in parallel,
        then aggregate timings and export report.
        """
        if not self.metrics:
            raise ValueError('No metrics added to matcher.')

        total_time = {m.name: 0.0 for m in self.metrics}

        with ProcessPoolExecutor(max_workers=self.max_workers) as exe:
            futures = {
                exe.submit(self._process_one, path, name, threshold, weights): name
                for path, name in self.list_path_tpub
            }

            # In verbose mode, we print all metrics as they complete so we don't want a progress bar.
            if self.verbose:
                iterator = as_completed(futures)

            # In non-verbose mode, we use tqdm to show progress.
            else:
                iterator = tqdm.tqdm(
                    as_completed(futures),
                    total=len(futures),
                    desc="Processing files",
                    unit="file",
                )

            failed = {}
            for fut in iterator:
                name = futures[fut]
                try:
                    timings = fut.result()
                except Exception as e:
                    failed[name] = repr(e)
                else:
                    for nm, t in timings.items():
                        total_time[nm] += t

        # A failed batch would silently drop candidate matches, so stop here
        if failed:
            raise RuntimeError(f"{len(failed)} target file(s) failed: {failed}")

        report = {
            'total_time_seconds': total_time,
            'weights': weights,
            'threshold': threshold,
        }
        self._export_report(report)

    def _export_report(self, report: dict) -> None:
        metrics_report = []
        for metric in self.metrics:
            cleaned = {
                k: v for k, v in vars(metric).items()
                if isinstance(v, (int, float, str))
                   and k not in ('apply_time', 'n_tpub')
            }
            metrics_report.append(cleaned)

        report.update({
            'metrics': metrics_report,
            'datestamp_process_end': pd.Timestamp.now().isoformat(),
            'path_qpub': self.path_qpub,
            'path_tpub_root': self.path_tpub_root,
            'path_match': self.path_match
        })
        with open(f'{self.path_match}/report_matches.json', 'w') as f:
            json.dump(report, f, indent=4)
        if self.verbose:
            print(json.dumps(report, indent=4))


class ReadMatch:
    """
    Helper function to read the match results from a Parquet file
    """

    def __init__(self, path_matches: str):
        """
        Initialize the ReadMatch instance with the path to the Parquet file.
        :param path: Path to the Parquet file containing match results.
        """
        self.path_matches = path_matches
        self.df = self._load(path_matches)
        self.col_metrics = [c for c in self.df.columns if c.startswith('metric_')]

    def add_priority_score(self, priority_score: PriorityScore):
        priority_score.apply(self.df)

    def _load(self, path: str):
        from pathlib import Path
        import pandas as pd

        p = Path(path)
        if p.is_dir():
            parts = []
            for fn in sorted(p.glob("*.parquet")):
                d = pd.read_parquet(fn)
                if not d.empty:
                    parts.append(d)  # use default engine=pyarrow
            if not parts:
                raise ValueError(f"No parquet files found in {path}.")
            df = pd.concat(parts, ignore_index=True)
        else:
            df = pd.read_parquet(path)

        if df.empty:
            raise ValueError(f'No match found in {path}. Please check the file.')
        print(f'Loaded {len(df)} match for {df["qpub_original_line_number"].nunique()} unique qpub from {path}')
        return df

    @staticmethod
    def _color_bool(val: bool) -> str:
        """
        Colorize a boolean value for better readability in terminal output.
        :param val: Boolean value to colorize.
        :return: Formatted string with colorized boolean value.
        """
        if val:
            return f'{Style.BRIGHT}{Fore.GREEN}True{Style.RESET_ALL}'
        else:
            return f'{Style.BRIGHT}{Fore.RED}False{Style.RESET_ALL}'

    def investigate(self, boolean_filter=None, limit=None):
        """
        Print a detailed investigation of the matches.
        :param boolean_filter: A boolean filter to apply to the DataFrame (e.g., to filter by a specific metric).
        :param limit: number of matches to display (default: 20).
        :return: None
        """
        if limit is None:
            limit = 20
        if boolean_filter is None:
            boolean_filter = np.ones(len(self.df), dtype=bool)

        ldf = self.df[boolean_filter]
        sample = ldf.sample(min(limit, len(ldf)))
        for _, r in sample.iterrows():
            block = f'''
{'#'*80}
TITLE:
    qpub_title : {r['qpub_title']}
    tpub_title : {r['tpub_title']}
    qpub_titleₙ : {Metric._normalize_text(r['qpub_title'])}
    tpub_titleₙ : {Metric._normalize_text(r['tpub_title'])}
   ➤ title_simhash  : {self._color_bool(r.get('metric_title_simhash'))}

AUTHORS:
    qpub_authors_all : {(';'.join(r['qpub_authors_all'])[:300])}
    tpub_authors_fullname : {(';'.join(r['tpub_authors_fullname'])[:300])}
    qpub_authors_allₙ : {str(MetricAuthorSimilarityMinHash._tokenize_list(r['qpub_authors_all']))[:300]}
    tpub_authors_fullnameₙ : {str(MetricAuthorSimilarityMinHash._tokenize_list(r['tpub_authors_fullname']))[:300]}
   ➤ author_jaccard : {self._color_bool(r.get('metric_author_jaccard'))}

DOI:
    qpub_dois : {r['qpub_dois']}
    tpub_doi : {r['tpub_doi']}
   ➤ doi_match      : {self._color_bool(r.get('metric_doi_match'))}

JOURNAL:
    qpub_journal : {r['qpub_journal']}
    tpub_primary_location : {r['tpub_primary_location']}
    qpub_journalₙ : {Metric._normalize_text(r['qpub_journal'])}
    tpub_primary_locationₙ : {Metric._normalize_text(r['tpub_primary_location'])}
   ➤ journal_exact  : {self._color_bool(r.get('metric_journal_exact_match'))}

AFFILIATION:
   ➤ metric_affiliated_to_institution: {self._color_bool(r.get('metric_affiliated_to_institution'))}
'''
            print(block)
