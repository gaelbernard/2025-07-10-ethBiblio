import os
import shutil

from modules.match.Match import MatchPublications
from modules.match.Metrics import MetricOpenAlexID, MetricTitleSimhash, MetricDoiMatch

'''
For each institution, compare its standardised publication list ({institution}/input/qpub.parquet)
with the OpenAlex works prepared in 1_data, and save every candidate match in {institution}/final_match/.
'''

institutions = ['ethz', 'epfl', 'empa', 'psi', 'wsl', 'eawag']
path_tpub = '../1_data/snapshot_in_parquets_2025_10_15'
output_dir = 'final_match'

if __name__ == "__main__":

    # Start from a clean output directory
    for institution in institutions:
        path_matches = f'{institution}/{output_dir}'
        shutil.rmtree(path_matches, ignore_errors=True)
        if os.path.exists(path_matches):
            raise Exception(f"Output directory {path_matches} already exists. Please delete it or change the output_dir.")

    for institution in institutions:
        print (f'** Processing {institution} **')

        path_qpub = f"{institution}/input/qpub.parquet"
        path_matches = f'{institution}/{output_dir}'

        assert os.path.exists(path_qpub), f"Query file {path_qpub} does not exist. Please run 01_prepare_input.py first."

        matcher = MatchPublications(path_qpub, path_tpub, path_matches, verbose=False, max_workers=2)
        matcher.add_metrics(MetricOpenAlexID())
        matcher.add_metrics(MetricTitleSimhash(k=3, f=128, min_length=12, ngram_size=2))
        matcher.add_metrics(MetricDoiMatch())

        # A candidate is kept as soon as one of the metrics matches
        weights = {
            'title_simhash': 1.0,
            'doi_match': 1.0,
            'openalex_id_match': 1.0,
        }
        matcher.run(threshold=1, weights=weights)
