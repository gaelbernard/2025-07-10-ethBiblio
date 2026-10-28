import time
import re
import unicodedata
import html
import urllib.parse
import pyarrow.parquet as pq
from simhash import Simhash, SimhashIndex
from collections import defaultdict
import pandas as pd
import numpy as np
from scipy import sparse

class Metric:
    """
    Base class for matching metrics between two publication sets.
    - qpub: query publications (the ones provided by the libraries)
    - tpub: target publications (the ones from OpenAlex snapshot). They are processed in batches of 50,000 records in parquet format.
    Subclasses must implement load_qpub() and apply().
    Typically, load_qpub() is executed once and should set self.query to a value that can be used in apply().
    This could be, for example, an inverted index of query titles, or a dictionary mapping DOIs to query indices.
    Apply() is executed for each target publication batch and should compute a sparse matrix of matches with shape (n_tpub, n_qpub),
    """

    # Useful regex patterns for text normalization
    _re_html = re.compile(r"<[^>]+>")
    _re_nonword = re.compile(r"[^\w\s-]")

    def __init__(self, name: str, description: str, verbose: bool = False):
        """
        Initialize the metric with a name and description.
        The metric is applied in two steps:
         - We call load_qpub(path_qpub) to load the query publications (only once)
         - We call run(path_tpub, name_tpub) to apply the metric to target publications. We call it for each parquet file in the target set.
        :param name: name of the metric (e.g., "title_exact", "affiliated_to_institution", etc.)
        :param description: description of the metric (e.g., "True if any author is affiliated to the institution.")
        :param verbose: if True, print metric application time and number of matches found.
        """
        self.name = name
        self.description = description
        self.query = None
        self.n_qpub: int = 0 # number of query publications
        self.n_tpub: int = 0 # number of target publications

        # The results of the metric application will be stored in this sparse matrix.
        # The number of rows corresponds to target publications, and the number of columns corresponds to query publications.
        self.match_matrix: sparse.csr_matrix | None = None
        self.apply_time: float = 0.0
        self.verbose = verbose

    def load_qpub(self, path_qpub: str):
        # Default: just store number of query publications (to be possibly overridden in subclasses)
        self.n_qpub = pq.read_table(path_qpub).num_rows
        self.query = None # This should be set by subclasses to a meaningful value depending on the metric (e.g., an inverted index, a list of titles, a institution ID, etc.)

    def run(self, path_tpub: str, name_tpub: str) -> sparse.csr_matrix:
        '''
        Run the metric on a target publication batch.
        :param path_tpub: Path to the target publication batch in parquet format.
        :param name_tpub: Name of the target publication batch (e.g., "batch_0a8eb6ea8d83474ab5d1ff3eff7dcb94.parquet").
        :return: A sparse matrix of matches with shape (n_tpub, n_qpub). It contains True for each match found between target and query publications.
        '''

        if self.query is None:
            raise RuntimeError("Query not loaded. Call load_qpub(path_qpub) first or make sure it sets self.query")
        self.n_tpub = pq.read_table(path_tpub).num_rows

        # Execute metric computation
        t0 = time.perf_counter()
        self.apply(path_tpub, name_tpub)
        self.apply_time = time.perf_counter() - t0

        # Sanity check on matrix shape
        assert self.match_matrix.shape == (self.n_tpub, self.n_qpub), f"{self.name} matrix shape must be ({self.n_tpub}, {self.n_qpub}), got {self.match_matrix.shape}"

        if self.verbose:
            print(f"[{self.name}] run: {self.apply_time:.3f}s. Got {self.match_matrix.nnz} matches.")

        return self.match_matrix

    def apply(self, path_tpub: str, name_tpub: str) -> sparse.csr_matrix:
        raise NotImplementedError("Subclasses must implement apply().")

    @staticmethod
    def _normalize_text(text: str) -> str:
        """
        Normalize a text. Since it is a pretty common operation, we implement it here and not in Metric subclasses.
        :param text: the text to normalize.
        :return: normalized text.
        """

        s = re.sub(r'<(?:sup|sub)>(.*?)</(?:sup|sub)>', r'\1', text)                    # Preserve any <sup>…</sup> or <sub>…</sub> contents
        s = Metric._re_html.sub(" ", s)                                                 # Strip _all_ other HTML tags and un-escape entities
        s = html.unescape(s)
        s = unicodedata.normalize("NFKD", s).lower()                                    # Unicode-normalize & lowercase
        s = re.sub(r'[-‐-‒–—−]+', '-', s)                                               # Turn every dash/‐/–/—/ → into ASCII hyphen
        s = re.sub(r'->|=>|→', '-', s)                                                  # Turn any “->”, “=>” or Unicode arrow into a hyphen
        s = s.replace('(', ' ').replace(')', ' ')                                       # Nuke any grouping punctuation
        s = s.replace('[', ' ').replace(']', ' ').replace('{', ' ').replace('}', ' ')
        s = re.sub(r'\s*-\s*', '-', s)                                                  # Collapse spaces _around_ hyphens so we have single “-” tokens
        s = Metric._re_nonword.sub(" ", s)                                              # Remove all punctuation except letters, digits, underscores, whitespace and hyphens
        s = re.sub(r'(?<=\w)-(?=\w)', "", s)                                            # Now strip hyphens that are just “minus signs” inside words:
        s = re.sub(r'-+', '-', s)                                                       # Remaining runs of hyphens collapsed
        s = re.sub(r'\s+', " ", s).strip()                                              # Final whitespace collapse
        return s


class MetricDoiMatch(Metric):
    '''
    True if query DOI matches target DOI. The query is a list of DOIs (provided by the libraries),
    and the target is a single DOI (from the OpenAlex snapshot).
    '''
    DOI_REGEX = re.compile(r'(10\.\d{4,9}/\S+)', re.IGNORECASE)

    def __init__(self):
        super().__init__(
            name="doi_match",
            description="True if query DOI matches target DOI."
        )

    def load_qpub(self, path_qpub: str):
        super().load_qpub(path_qpub)
        # Read all DOIs for query publications and build a normalized DOI → query index map
        qdoi_lists = pd.read_parquet(path_qpub, columns=["dois"])["dois"].tolist()
        doi_map = defaultdict(list)
        for j, doi_list in enumerate(qdoi_lists):
            for raw in doi_list:
                cd = self.clean_doi(raw)
                if cd:
                    doi_map[cd].append(j)

        self.query = doi_map

    @staticmethod
    def clean_doi(raw: str) -> str | None:
        if not raw or not isinstance(raw, str):
            return None
        s = raw.strip()
        # Remove common DOI URL prefixes
        for prefix in (
            "https://doi.org/", "http://doi.org/",
            "https://dx.doi.org/", "http://dx.doi.org/",
            "doi:"
        ):
            if s.lower().startswith(prefix):
                s = s[len(prefix):]
                break
        # Remove URL query or fragment
        s = s.split("?", 1)[0].split("#", 1)[0]
        s = urllib.parse.unquote(s)
        # Strip trailing punctuation and whitespace
        s = s.rstrip(" .;,/<>")
        m = MetricDoiMatch.DOI_REGEX.search(s)
        return m.group(1).lower() if m else None

    def apply(self, path_tpub: str, name_tpub: str) -> sparse.csr_matrix:
        # Read target DOIs and normalize them
        raw_t = pd.read_parquet(path_tpub, columns=["doi"])["doi"].tolist()
        target_clean = [self.clean_doi(d) for d in raw_t]
        rows, cols = [], []
        # For each target DOI, find matching query indices via the precomputed map
        for i, cd in enumerate(target_clean):
            if cd is None:
                continue
            for j in self.query.get(cd, []):
                rows.append(i)
                cols.append(j)
        data = np.ones(len(rows), dtype=bool)
        self.match_matrix = sparse.csr_matrix(
            (data, (rows, cols)),
            shape=(self.n_tpub, self.n_qpub),
            dtype=bool
        )
        return self.match_matrix

class MetricOrcidOverlap(Metric):
    """
    True if target publication and query publication share at least one ORCID.
    """
    def __init__(self):
        super().__init__(
            name="orcid_overlap",
            description="True if target publication and query publication share at least one ORCID."
        )
        self.orcid_map: dict[str, list[int]] = {}

    def load_qpub(self, path_qpub: str):
        super().load_qpub(path_qpub)
        # Read ORCIDs for all query publications and build ORCID → query indices map
        qorcid_lists = pd.read_parquet(path_qpub, columns=["orcids"])["orcids"].tolist()
        orcid_map = defaultdict(list)
        for j, orcids in enumerate(qorcid_lists):
            for o in orcids:
                if o:  # ensure non-empty ORCID string
                    orcid_map[o].append(j)
        self.orcid_map = orcid_map
        self.query = {'orcid_map': self.orcid_map}

    def apply(self, path_tpub: str, name_tpub: str) -> sparse.csr_matrix:
        # Read target author ORCIDs
        target_orcids = pd.read_parquet(path_tpub, columns=["author_orcids"])["author_orcids"].tolist()
        rows, cols = [], []
        for i, orcid_list in enumerate(target_orcids):
            for o in orcid_list:
                if not o:
                    continue
                for j in self.orcid_map.get(o, []):
                    rows.append(i)
                    cols.append(j)
        data = np.ones(len(rows), dtype=bool)
        self.match_matrix = sparse.csr_matrix(
            (data, (rows, cols)),
            shape=(self.n_tpub, self.n_qpub),
            dtype=bool
        )
        return self.match_matrix

class MetricTitleExact(Metric):
    """
    True if the normalized title of a target publication matches any normalized title of a query publication.
    It uses a reverse index to speed up the matching process
    """

    def __init__(self, min_length: int = 20):
        """
        Initialize the metric with a minimum length for titles.
        :param min_length: disregard titles shorter than this length after normalization
        """
        super().__init__(
            name="title_exact",
            description="Exact title match (normalized, min length)."
        )
        self.min_length = min_length

    def load_qpub(self, path_qpub: str):
        super().load_qpub(path_qpub)
        # Read query titles and build normalized index
        titles = pd.read_parquet(path_qpub, columns=["title"])["title"].fillna("").tolist()
        q_index = defaultdict(list)
        for j, t in enumerate(titles):
            norm = self._normalize_text(t)
            if len(norm) < self.min_length or norm == "":
                continue
            q_index[norm].append(j)
        self.query = q_index

    def apply(self, path_tpub: str, name_tpub: str) -> sparse.csr_matrix:

        titles = pd.read_parquet(path_tpub, columns=["title"])["title"].fillna("").tolist()
        rows, cols = [], []
        for i, t in enumerate(titles):
            norm = self._normalize_text(t)
            if len(norm) < self.min_length or norm == "":
                continue
            if norm in self.query:
                for j in self.query[norm]:
                    rows.append(i)
                    cols.append(j)
        data = np.ones(len(rows), dtype=bool)
        self.match_matrix = sparse.csr_matrix(
            (data, (rows, cols)),
            shape=(self.n_tpub, self.n_qpub),
            dtype=bool
        )
        return self.match_matrix

class MetricTitleSimhash(Metric):
    """
    True if the normalized title of a target publication is similar to any normalized title of a query publication.
    It uses SimHash to compute a fuzzy match based on character n-grams which allows for small typos and variations.
    """

    def __init__(self, k: int = 3, f: int = 64, min_length: int = 20, ngram_size: int = 3):
        super().__init__(
            name="title_simhash",
            description="SimHash fuzzy title match."
        )
        self.k = k
        self.f = f
        self.min_length = min_length
        self.ngram_size = ngram_size
        self.index: SimhashIndex | None = None

    def _char_ngrams(self, s: str) -> set[str]:
        pad = " " * (self.ngram_size - 1)
        text = f"{pad}{s}{pad}"
        return { text[i:i+self.ngram_size] for i in range(len(text) - self.ngram_size + 1) }

    def load_qpub(self, path_qpub: str):
        super().load_qpub(path_qpub)
        # Build Simhash objects for each query title and add to index
        titles = pd.read_parquet(path_qpub, columns=["title"])["title"].fillna("").tolist()
        simhash_objs = []
        for j, t in enumerate(titles):
            norm = self._normalize_text(t)
            if len(norm) < self.min_length or norm == "":
                continue
            sim = Simhash(self._char_ngrams(norm), f=self.f)
            simhash_objs.append((str(j), sim))
        # Build SimhashIndex with tolerance k
        self.index = SimhashIndex(simhash_objs, f=self.f, k=self.k)
        self.query = {'simhash_index': self.index}

    def apply(self, path_tpub: str, name_tpub: str) -> sparse.csr_matrix:
        titles = pd.read_parquet(path_tpub, columns=["title"])["title"].fillna("").tolist()
        rows, cols = [], []
        # Use a cache for normalization and simhash within this chunk to avoid duplicate computation
        for i, t in enumerate(titles):
            norm = self._normalize_text(t)
            if len(norm) < self.min_length or norm == "":
                sim = None
            else:
                sim = Simhash(self._char_ngrams(norm), f=self.f)

            if sim is None:
                continue  # title too short or empty after normalization
            # Find near-duplicates in query via simhash index
            for qid in self.index.get_near_dups(sim):
                rows.append(i)
                cols.append(int(qid))

        data = np.ones(len(rows), dtype=bool)
        self.match_matrix = sparse.csr_matrix(
            (data, (rows, cols)),
            shape=(self.n_tpub, self.n_qpub),
            dtype=bool
        )
        return self.match_matrix

class MetricAuthorSimilarityMinHash(Metric):
    """
    True if the Jaccard similarity of author lists between target and query publications is above a threshold.
    The preprocessing of the author names is key to ensure that similar names are matched correctly.
    We then use MinHash to efficiently compute Jaccard similarity between sets of author names.
    """
    def __init__(self, threshold: float = 0.8):
        super().__init__(
            name="author_jaccard",
            description="Author-list Jaccard ≥ threshold."
        )
        self.threshold = threshold


    @staticmethod
    def _simplify_name(name: str) -> str | None:
        """
        Simplify an author name to a token of the form "first-initial last-name".
        :param name: name to simplify. It can be in the form "Last, First" or "First Last" or "First-Last".
        :return: simplified token or None if the name is invalid (e.g., too short, no last name).
        """
        # Simplify an author name to "first-initial last-name"
        if not name or not isinstance(name, str):
            return None
        s = name.strip()
        # Convert "Last, First" to "First Last"
        if ',' in s:
            parts = s.split(',', 1)
            if len(parts) == 2:
                last, first = parts[0].strip(), parts[1].strip()
                s = f"{first} {last}"
        # Remove accents and non-ASCII characters
        nfkd = unicodedata.normalize("NFKD", s)
        clean = nfkd.encode("ASCII", "ignore").decode()
        # Keep only letters and spaces
        parts = re.sub(r"[^\w\s]", "", clean).split()
        if len(parts) < 2:
            # If we cannot get at least first and last name parts, consider it invalid for tokenization
            return None
        first, last = parts[0], parts[-1]
        # Use first part's first initial (or first letter of each hyphenated part) and last name
        initials = ''.join(p[0] for p in first.split('-') if p) or first[0]
        token = f"{initials.lower()} {last.lower()}"
        # Store in cache
        return token

    @staticmethod
    def _tokenize_list(names: list[str]) -> set[str]:
        # Convert a list of author names to a set of simplified tokens
        tokens = set()
        for name in names:
            token = MetricAuthorSimilarityMinHash._simplify_name(name)
            if token:
                tokens.add(token)
        return tokens

    def load_qpub(self, path_qpub: str):
        super().load_qpub(path_qpub)
        # Read all query authors and compute token sets
        names = pd.read_parquet(path_qpub, columns=["authors_all"])["authors_all"]
        token_sets = names.apply(self._tokenize_list).tolist()
        self.n_qpub = len(token_sets)
        self.query = {
            'token_sets': token_sets,
            'counts_q': np.array([len(s) for s in token_sets], dtype=int),
            'inv_index': {}
        }
        # Build inverted index: token -> list of query indices
        inv = {}
        for q_idx, toks in enumerate(token_sets):
            for tok in toks:
                inv.setdefault(tok, []).append(q_idx)
        self.query['inv_index'] = inv

    def apply(self, path_tpub: str, name_tpub: str) -> sparse.csr_matrix:
        # Load target author lists
        tnames = pd.read_parquet(path_tpub, columns=["authors_fullname"])["authors_fullname"].tolist()
        n_tpub = len(tnames)
        # Tokenize target lists and record counts
        tok_t = []
        counts_t = np.zeros(n_tpub, dtype=int)
        for i, names in enumerate(tnames):
            toks = self._tokenize_list(names)
            tok_t.append(toks)
            counts_t[i] = len(toks)
        # Gather all unique tokens in target set
        all_tokens = sorted({tok for toks in tok_t for tok in toks})
        tok_idx = {tok: idx for idx, tok in enumerate(all_tokens)}
        # Build M_t: target incidence matrix (rows=targets, cols=tokens)
        rows, cols = [], []
        for i, toks in enumerate(tok_t):
            for tok in toks:
                rows.append(i)
                cols.append(tok_idx[tok])
        data = np.ones(len(rows), dtype=int)
        M_t = sparse.csr_matrix((data, (rows, cols)), shape=(n_tpub, len(all_tokens)))
        # Build M_q: query incidence matrix (rows=queries, cols=tokens) using inverted index
        rows_q, cols_q = [], []
        for tok, col in tok_idx.items():
            for qj in self.query['inv_index'].get(tok, []):
                rows_q.append(qj)
                cols_q.append(col)
        data_q = np.ones(len(rows_q), dtype=int)
        M_q = sparse.csr_matrix((data_q, (rows_q, cols_q)), shape=(self.n_qpub, len(all_tokens)))
        # Compute intersections via sparse matrix multiplication
        I = (M_t @ M_q.T).tocoo()
        r, c, inter = I.row, I.col, I.data
        # Compute unions and apply threshold
        unions = counts_t[r] + self.query['counts_q'][c] - inter
        mask = inter >= self.threshold * unions
        r_keep = r[mask]
        c_keep = c[mask]
        data_bool = np.ones(len(r_keep), dtype=bool)
        # Build boolean match matrix
        self.match_matrix = sparse.csr_matrix(
            (data_bool, (r_keep, c_keep)),
            shape=(n_tpub, self.n_qpub), dtype=bool
        )
        return self.match_matrix

class MetricJournalTitleExact(Metric):
    """
    True if the normalized journal title of a target publication matches
    any normalized journal title of a query publication.
    """

    def __init__(self, min_length: int = 3):
        super().__init__(
            name="journal_exact_match_primary_location",
            description="Exact journal match."
        )
        self.min_length = min_length

    # --- helpers ---
    def _prep_series(self, s: pd.Series) -> pd.Series:
        s = s.fillna("").astype(str)
        s = s.apply(self._normalize_text)              # normalize first
        s = s[s.str.len() >= self.min_length]          # then length filter
        return s

    def load_qpub(self, path_qpub: str):
        super().load_qpub(path_qpub)

        q_j = pd.read_parquet(path_qpub, columns=["journal"])["journal"]
        q_j = self._prep_series(q_j)

        q_map = defaultdict(list)
        for idx, val in q_j.items():
            q_map[val].append(idx)

        self.query = {
            "map": q_map,                   # normalized title -> list of q indices
            "keys": set(q_map.keys()),
        }

    def apply(self, path_tpub: str, name_tpub: str) -> sparse.csr_matrix:
        t_j = pd.read_parquet(path_tpub, columns=["primary_location"])["primary_location"]
        t_j = self._prep_series(t_j)

        t_map = defaultdict(list)
        for idx, val in t_j.items():
            t_map[val].append(idx)

        rows, cols = [], []
        common = t_map.keys() & self.query["keys"]
        q_map = self.query["map"]

        for s in common:
            t_idxs = t_map[s]
            q_idxs = q_map[s]
            # cartesian product of duplicates
            for ti in t_idxs:
                rows.extend([ti] * len(q_idxs))
                cols.extend(q_idxs)

        data = np.ones(len(rows), dtype=bool)
        self.match_matrix = sparse.csr_matrix(
            (data, (rows, cols)),
            shape=(self.n_tpub, self.n_qpub),
            dtype=bool
        )
        return self.match_matrix

class MetricYearMatch(Metric):
    """
    True if the publication years differ by at most the specified threshold.
    This metric is not efficient because it's not specific enough.
    Typically if the max_diff is 1 and we study a 10 year period, a publication has 1/3 to match with any publications
    Storing all the matches is not efficient. I would recommend not using this metric.
    """
    def __init__(self, max_diff: int = 0, chunk_size: int = 1024):
        super().__init__(
            name="year_match",
            description=f"Year difference ≤ {max_diff}."
        )
        self.max_diff = max_diff
        # Chunk size for vectorized comparison
        self.chunk_size = chunk_size
        self.years_q = None

    def load_qpub(self, path_qpub: str):
        super().load_qpub(path_qpub)
        # Load and store query years as NumPy array
        years = pd.read_parquet(path_qpub, columns=["year"])["year"]
        # Fill missing with sentinel outside any reasonable diff range
        years = years.fillna(-10**9).astype(int)
        self.query = years.to_numpy()

    def apply(self, path_tpub: str, name_tpub: str) -> sparse.csr_matrix:
        # Load target years as NumPy array
        years_t = pd.read_parquet(path_tpub, columns=["publication_year"])["publication_year"]
        years_t = years_t.fillna(10**9).astype(int).to_numpy()

        n_t = len(years_t)
        n_q = len(self.query)
        rows, cols = [], []

        # Year difference for every (tpub, qpub) pair, computed at once
        diff = np.abs(years_t[:, None] - self.query[None, :])
        # Find positions where diff <= max_diff
        i_idx, j_idx = np.nonzero(diff <= self.max_diff)
        if i_idx.size:
            rows.extend(i_idx.tolist())
            cols.extend(j_idx.tolist())

        if rows:
            rows = np.array(rows, dtype=int)
            cols = np.array(cols, dtype=int)
            data = np.ones(len(rows), dtype=bool)
            self.match_matrix = sparse.csr_matrix(
                (data, (rows, cols)),
                shape=(n_t, n_q),
                dtype=bool
            )
        else:
            self.match_matrix = sparse.csr_matrix((n_t, n_q), dtype=bool)

        return self.match_matrix

class MetricOpenAlexID(Metric):
    """
    True if the OpenAlex ID of a target publication matches any OpenAlex ID of a query publication.
    """

    def __init__(self):
        super().__init__(
            name="openalex_id_match",
            description="Retrieving publication using the OpenAlex ID."
        )

    def load_qpub(self, path_qpub: str):
        super().load_qpub(path_qpub)
        # OpenAlex ID -> positions in the query list (an ID can appear on several lines)
        openalex_ids = pd.read_parquet(path_qpub, columns=["openalex_id"])["openalex_id"].reset_index(drop=True).dropna().astype(int)
        inverse_index = defaultdict(list)
        for j, openalex_id in openalex_ids.items():
            inverse_index[openalex_id].append(j)
        self.query = dict(inverse_index)

    def apply(self, path_tpub: str, name_tpub: str) -> sparse.csr_matrix:
        ids = pd.read_parquet(path_tpub, columns=["id"])["id"].astype(int).tolist()
        rows, cols = [], []
        for i, openalex_id in enumerate(ids):
            for j in self.query.get(openalex_id, []):
                rows.append(i)
                cols.append(j)

        data = np.ones(len(rows), dtype=bool)
        self.match_matrix = sparse.csr_matrix(
            (data, (rows, cols)),
            shape=(self.n_tpub, self.n_qpub),
            dtype=bool
        )
        return self.match_matrix


class MetricAffiliatedToInstitution(Metric):
    """
    True if any author is affiliated to the specified institution.
    This metric is a bit particular because it does not require any query data (just the institution ID).
    """
    def __init__(self, institution_id: int):
        """
        Initialize the metric to check affiliation to a specific institution.
        :param institution_id: id of the institution from OpenAlex.
        Please remove any non-numerical character (e.g., 5124864 for EPFL and not i5124864 =>  https://openalex.org/institutions/i5124864)
        """
        super().__init__(
            name="affiliated_to_institution",
            description="True if any author is affiliated to the institution."
        )
        assert isinstance(institution_id, int) and institution_id > 0, "institution_id must be a positive integer."
        self.institution_id = institution_id

    def load_qpub(self, path_qpub: str):
        super().load_qpub(path_qpub)
        self.query = self.institution_id    # Use institution_id directly as query data

    def apply(self, path_tpub: str, name_tpub: str) -> sparse.csr_matrix:
        # Load and explode so each publication-institution pair is its own row
        df = pd.read_parquet(path_tpub, columns=['institutions_ids'])
        exploded = df.explode('institutions_ids')

        # Find all target rows matching our institution_id
        matched_rows = exploded.index[exploded['institutions_ids'] == self.institution_id].unique()

        # If none matched, return an all-False matrix
        n_q = self.n_qpub
        if matched_rows.size == 0:
            self.match_matrix = sparse.csr_matrix((self.n_tpub, n_q), dtype=bool)
            return self.match_matrix

        # Otherwise, set every matched row to True for _all_ query columns
        row_idx = np.repeat(matched_rows, n_q)
        col_idx = np.tile(np.arange(n_q), matched_rows.size)
        data = np.ones_like(row_idx, dtype=bool)

        self.match_matrix = sparse.csr_matrix(
            (data, (row_idx, col_idx)),
            shape=(self.n_tpub, n_q),
            dtype=bool
        )
        return self.match_matrix