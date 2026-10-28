import pandas as pd
import re
import ast
import json
import warnings
import numpy as np

'''
This class is responsible for loading, processing, and validating a list of publications provided by the libraries.
Using this class, you ensure that the data adheres to a specific schema and format (e.g., required fields, list fields, DOI format).
'''

class PublicationList:
    REQUIRED_FIELDS = {
        "title", "internal_id", "dois", "openalex_id", "year",
        "publication_type", "authors_all",
        "journal"
    } # These are the required fields that must be present in the data.

    LIST_FIELDS = [
        "dois",
        "authors_institution",
        "authors_all",
        "orcid_institution"
    ] # These fields will be treated as lists, even if they contain a single item or are empty (e.g., "10.1234/abcd" will become "['10.1234/abcd']").

    def __init__(self, df, path_output_parquet):
        '''
        Initialize the PublicationList instance.
        '''
        self.df = df
        self.check()

        # Add a column named original_line_number to keep track of the original line number in the input file.
        # Add this column at the beginning of the DataFrame.
        if 'original_line_number' in self.df.columns:
            self.df.drop('original_line_number', axis=1, inplace=True)
        self.df.insert(0, 'original_line_number', range(1, len(self.df) + 1))

        self.df.to_parquet(path_output_parquet, index=False)

    def check(self):
        self._validate_required_fields()
        self._preprocess_list_fields()

        self._remove_prefix_doi()
        print ()
        print ('############################################')
        print ("Validating the publication list, the information printed below is for debugging purposes (making sure the data is well parsed):")
        self._validate_authors_list()
        self._validate_openalex_id()
        self._validate_journals()
        print ('showing first 5 records')
        print (self.df.head().to_string())
        print ('showing last 5 records')
        print (self.df.tail().to_string())
        print ('\t END OF VALIDATION')

    def _apply_column_mapping(self, mapping):
        if mapping:
            self.df.rename(columns=mapping, inplace=True)

    def _validate_required_fields(self):
        if self.df is None:
            raise ValueError("No data loaded. Use load_csv, load_json, or load_df first.")
        missing = self.REQUIRED_FIELDS - set(self.df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    def _preprocess_list_fields(self):
        for col in self.LIST_FIELDS:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(self._parse_list_like)

    def _parse_list_like(self, val):
        if isinstance(val, list):
            return val
        if isinstance(val, (np.ndarray, pd.Series)):
            return val.tolist()

        if val is None or (isinstance(val, str) and val.strip() == ""):
            return []
        if isinstance(val, (np.ndarray, pd.Series)) and val.size == 0:
            return []
        if pd.isna(val):
            return []

        s = str(val).strip()

        # 1) JSON list
        if s.startswith('[') and s.endswith(']'):
            try:
                return json.loads(s)
            except json.JSONDecodeError:
                pass

        # 2) Python literal list (guard with warnings filter)
        if s.startswith('[') and s.endswith(']'):
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=SyntaxWarning)
                    parsed = ast.literal_eval(s)
                if isinstance(parsed, list):
                    return parsed
            except (ValueError, SyntaxError):
                pass

        # 3) Known delimiters (tune order to your data)
        for delim in ('||', ';', '|', ','):
            if delim in s:
                items = [x.strip() for x in s.split(delim)]
                return [x for x in items if x]

        # 4) Fallback: single item
        return [s]

    def _remove_prefix_doi(self):
        prefix_regex = re.compile(r"^(https?://)?(dx\.)?doi\.org/", re.IGNORECASE)
        cleaned = []
        for i, lst in enumerate(self.df["dois"]):
            if not isinstance(lst, list):
                raise ValueError(f"'dois' must be a list in row {i}: {lst}")
            new = []
            for d in lst:
                if not isinstance(d, str):
                    raise ValueError(f"DOI must be string in row {i}: {d}")
                d_clean = prefix_regex.sub("", d).strip()
                new.append(d_clean)
            cleaned.append(new)
        self.df["dois"] = cleaned

    def _validate_openalex_id(self):
        '''
        The OpenAlex ID should be an integer or null.
        We perform a check to ensure that the OpenAlex IDs are integers and within the expected range.
        :return:
        '''
        # This will raise an error if conversion fails since we expect integers (no effect on the DF is voluntary)
        try:
            self.df["openalex_id"].dropna().astype(int)
        except ValueError:
            raise ValueError("OpenAlex IDs must be integers or null. Found non-integer values in 'openalex_id' column.")
        if not self.df['openalex_id'].isnull().all():
            # The average of the OpenAlex IDs should be between 9 or 12 digits, which is the expected length of OpenAlex IDs.
            avg_length = self.df["openalex_id"].dropna().astype(str).apply(len).mean().astype(int)

            assert (avg_length >= 9 and avg_length <= 12), 'OpenAlex IDs should be between 9 and 12 digits long on average. Found average length: {}. Make sure the OpenAlex id is correct'.format(avg_length)

            print (f'\t - The first five OpenAlex ids are: {self.df["openalex_id"].dropna().head().astype(int).tolist()}')
            print (f'\t - The last five OpenAlex ids are: {self.df["openalex_id"].dropna().tail().astype(int).tolist()}')

    def _validate_authors_list(self):
        '''
        Validate that the authors_institution and authors_all fields are lists of strings.
        This is some test to make sure the list of authors is well parsed and contains the expected number of authors.
        :return:
        '''
        # make sure that authors_institution and authors_all are lists of strings
        if 'authors_institution' in self.df.columns:
            assert self.df['authors_institution'].apply(lambda x: isinstance(x, list) and all(isinstance(i, str) for i in x)).all(), "authors_institution must be a list of strings"
        # The median of number of authors should be between 2 and 10 authors, which is the expected number of authors in a publication.
        median_length = self.df['authors_all'].str.len().median()
        assert (median_length >= 2 and median_length <= 10), 'The median number of authors should be between 2 and 10. Found median length: {}. Make sure the authors_all field is correct'.format(median_length)

        # We print the first values of authors_institution and authors_all to check if they are well parsed
        try:
            print (f'\t - The first author of the first publication is: {self.df["authors_all"].iloc[0][0]}. The complete list of authors for this publication is: {self.df["authors_all"].iloc[0]}')
        except IndexError:
            pass
        try:
            print (f'\t - The first author of the last publication is: {self.df["authors_all"].iloc[-1][0]}. The complete list of authors for this publication is: {self.df["authors_all"].iloc[-1]}')
        except:
            pass

    def _validate_journals(self):
        '''
        Validate that the journal field is a list of strings.
        This is some test to make sure the list of journals is well parsed and contains the expected number of journals.
        :return:
        '''
        assert ~self.df['journal'].str.startswith('[').all(), "journals starting with '[' is suspicious, make sure the journal field is correct"