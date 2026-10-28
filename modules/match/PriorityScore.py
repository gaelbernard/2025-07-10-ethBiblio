import os
import re
import unicodedata
import html
import pyarrow.parquet as pq
import pandas as pd
from rapidfuzz.distance.DamerauLevenshtein import normalized_distance as damerau_levenshtein
import tqdm


class PriorityScore:
    """
    Base class for scores computed on candidate matches (one row per qpub/tpub pair).
    apply(df) adds a column named after the score to the candidate DataFrame.
    """

    # Useful regex patterns for text normalization
    _re_html = re.compile(r"<[^>]+>")
    _re_nonword = re.compile(r"[^\w\s-]")

    def __init__(self, name, description: str, verbose: bool = False):
        self.name = name

    def apply(self, df) -> None:
        raise NotImplementedError("Subclasses must implement apply().")

    @staticmethod
    def _normalize_text(text: str) -> str:
        """
        Normalize a text. Since it is a pretty common operation, we implement it here and not in Metric subclasses.
        :param text: the text to normalize.
        :return: normalized text.
        """

        s = re.sub(r'<(?:sup|sub)>(.*?)</(?:sup|sub)>', r'\1', text)                    # Preserve any <sup>…</sup> or <sub>…</sub> contents
        s = PriorityScore._re_html.sub(" ", s)                                                 # Strip _all_ other HTML tags and un-escape entities
        s = html.unescape(s)
        s = unicodedata.normalize("NFKD", s).lower()                                    # Unicode-normalize & lowercase
        s = re.sub(r'[-‐-‒–—−]+', '-', s)                                               # Turn every dash/‐/–/—/ → into ASCII hyphen
        s = re.sub(r'->|=>|→', '-', s)                                                  # Turn any “->”, “=>” or Unicode arrow into a hyphen
        s = s.replace('(', ' ').replace(')', ' ')                                       # Nuke any grouping punctuation
        s = s.replace('[', ' ').replace(']', ' ').replace('{', ' ').replace('}', ' ')
        s = re.sub(r'\s*-\s*', '-', s)                                                  # Collapse spaces _around_ hyphens so we have single “-” tokens
        s = PriorityScore._re_nonword.sub(" ", s)                                              # Remove all punctuation except letters, digits, underscores, whitespace and hyphens
        s = re.sub(r'(?<=\w)-(?=\w)', "", s)                                            # Now strip hyphens that are just “minus signs” inside words:
        s = re.sub(r'-+', '-', s)                                                       # Remaining runs of hyphens collapsed
        s = re.sub(r'\s+', " ", s).strip()                                              # Final whitespace collapse
        return s



class AuthorsSimilarity(PriorityScore):
    """
    Similarity (0-1) between the author lists of the query and target publications.
    Each name is simplified to "initials last-name", and the sorted lists of simplified names
    are compared with a normalized Damerau-Levenshtein distance.
    """
    def __init__(self):
        super().__init__(
            name="authors_similarity",
            description=""
        )

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
            token = AuthorsSimilarity._simplify_name(name)
            if token:
                tokens.add(token)
        return tokens

    def _tokens_as_string(self, names) -> str:
        # Sorted so that the string (and therefore the edit distance) does not depend on set ordering
        if names is None:
            return ''
        return ', '.join(sorted(self._tokenize_list(names)))

    def apply(self, df):
        qpub_authors = df['qpub_authors_all'].apply(self._tokens_as_string)
        tpub_authors = df['tpub_authors_fullname'].apply(self._tokens_as_string)
        df[self.name] = _string_similarity(qpub_authors, tpub_authors)


def _string_similarity(q: pd.Series, t: pd.Series, max_length: int = 1500) -> list[float]:
    """
    1 - normalized Damerau-Levenshtein distance for each pair of strings (0.0 when one of them is empty).
    """
    scores = []
    for s1, s2 in tqdm.tqdm(zip(q.tolist(), t.tolist()), total=len(q)):
        if not s1 or not s2:
            scores.append(0.0)
            continue
        scores.append(1.0 - damerau_levenshtein(s1[0:max_length], s2[0:max_length]))
    return scores


class JournalSimilarity(PriorityScore):

    def __init__(self):
        super().__init__(
            name="journal_similarity",
            description=""
        )

    def apply(self, df):
        q = df['qpub_journal'].fillna('').astype(str).apply(self._normalize_text)
        t = df['tpub_primary_location'].fillna('').astype(str).apply(self._normalize_text)
        df[self.name] = _string_similarity(q, t)


class TitleSimilarity(PriorityScore):

    def __init__(self):
        super().__init__(
            name="title_similarity",
            description=""
        )

    def apply(self, df):
        q = df['qpub_title'].fillna('').astype(str).apply(self._normalize_text)
        t = df['tpub_title'].fillna('').astype(str).apply(self._normalize_text)
        df[self.name] = _string_similarity(q, t)


class RareTitle(PriorityScore):
    """
    True if the normalized query title occurs at most `max_occurence` times among the target titles (OpenAlex snapshot).

    Counting requires a full pass over the target files, so counts are cached in `cache_path`
    (a dict: normalized title -> number of occurrences). Titles missing from the cache are counted
    on demand. Call `prepare()` with the titles of all publication lists beforehand to do a single pass.
    The cache is specific to one snapshot: by default it is stored next to the target folder.
    """

    def __init__(self, path_target, max_occurence=1, cache_path=None):
        super().__init__(
            name="rare_title",
            description=""
        )
        self.path_target = path_target
        self.max_occurence = max_occurence
        self.cache_path = cache_path or f"{str(path_target).rstrip('/')}_title_counts.pickle"

    def _load_cache(self) -> dict:
        if os.path.exists(self.cache_path):
            return pd.read_pickle(self.cache_path)
        return {}

    def _count_titles(self, titles: set) -> dict:
        counts = dict.fromkeys(titles, 0)
        files = sorted(x for x in os.listdir(self.path_target) if x.endswith('.parquet'))
        for file in tqdm.tqdm(files, total=len(files), desc='Counting occurrences of titles'):
            target_title = pq.read_table(os.path.join(self.path_target, file), columns=['title']).to_pandas()['title'].fillna('').astype(str).apply(self._normalize_text)
            for title, n in target_title[target_title.isin(titles)].value_counts().items():
                counts[title] += int(n)
        return counts

    def prepare(self, titles) -> dict:
        """
        Make sure every title in `titles` (raw, not normalized) has a count in the cache, and return the cache.
        """
        normalized = set(pd.Series(list(titles)).fillna('').astype(str).apply(self._normalize_text))
        counts = self._load_cache()
        missing = normalized - counts.keys()
        if missing:
            counts.update(self._count_titles(missing))
            pd.to_pickle(counts, self.cache_path)
        return counts

    def apply(self, df):
        counts = self.prepare(df['qpub_title'])
        query_title = df['qpub_title'].fillna('').astype(str).apply(self._normalize_text)
        df[self.name] = [counts[x] <= self.max_occurence for x in query_title]
