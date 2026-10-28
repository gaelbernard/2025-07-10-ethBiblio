import pandas as pd

from modules.match.Match import ReadMatch
from modules.match.PriorityScore import TitleSimilarity, RareTitle, JournalSimilarity, AuthorsSimilarity

'''
For each institution, score the candidate matches found by 02_discover_matches.py, drop low-confidence
candidates, and keep the best OpenAlex work per publication of the input list.
'''

institutions = ['empa', 'psi', 'wsl', 'epfl', 'ethz', 'eawag']
path_tpub = '../1_data/snapshot_in_parquets_2025_10_15'

# Count how often each title of the six lists occurs in the snapshot (single pass, cached)
rare_title = RareTitle(path_tpub, max_occurence=1)
rare_title.prepare(pd.concat([
    pd.read_parquet(f"{institution}/input/qpub.parquet", columns=['title'])['title']
    for institution in institutions
]))

for institution in institutions:
    print ()
    print(f'** Processing {institution} **')

    path_qpub = f"{institution}/input/qpub.parquet"
    path_matches = f'{institution}/final_match/parquets'
    path_final_matches = f'{institution}/final_match/final_list_all_matches.parquet'
    path_final_top_match = f'{institution}/final_match/final_list_top_match.parquet'

    match = ReadMatch(path_matches)
    match.add_priority_score(TitleSimilarity())
    match.add_priority_score(rare_title)
    match.add_priority_score(JournalSimilarity())
    match.add_priority_score(AuthorsSimilarity())
    df = match.df

    # Drop retracted works
    df = df[df['tpub_is_retracted'] == False]

    # Keep candidates found by OpenAlex ID or DOI. Candidates found by title only are kept if the title
    # is similar and either rare in the snapshot or confirmed by both the journal and the authors.
    df = df[
        ( df['metric_openalex_id_match'] == True ) |
        ( df['metric_doi_match'] == True ) |
        ( (df['title_similarity'] >= 0.7) & (df['rare_title'] == True) ) |
        ( (df['title_similarity'] >= 0.7) & (df['journal_similarity'] >= 0.7) & (df['authors_similarity'] >= 0.7) )
    ].copy()
    print(f'{len(df):,} candidates kept')

    # In-scope flags of the OpenAlex work (see OpenAlexSnapshot.extract_work_flags)
    cols_flags = [x for x in df.columns if x.startswith('tpub_flag_')]
    df['flag_all_true'] = df[cols_flags].all(axis=1)

    df['same_year'] = (df['qpub_year'] == df['tpub_publication_year']).fillna(False).astype(bool)

    df['priority_score'] = (df['flag_all_true']*4) + df['journal_similarity'] + df['authors_similarity'] + df['title_similarity'] + df['metric_openalex_id_match'] + df['metric_doi_match'] + df['same_year']

    # Add back the publications without any candidate
    found_set = set(df['qpub_original_line_number'].tolist())
    qpub = pd.read_parquet(path_qpub).add_prefix('qpub_')
    qpub['rule_FINAL'] = 'not_found'

    not_found = qpub[~qpub['qpub_original_line_number'].isin(found_set)]
    concat = df
    if not not_found.empty:
        concat = pd.concat([df, not_found])

    # Order the rows like the input, best candidate first (ties broken by OpenAlex ID for reproducibility)
    concat = concat.sort_values(by=['qpub_original_line_number', 'priority_score', 'tpub_id'], ascending=[True, False, True], kind='mergesort')
    concat['priority_score'] = concat['priority_score'].fillna(0)

    concat['priority_order'] = 1
    concat['priority_order'] = concat.groupby('qpub_original_line_number')['priority_order'].cumsum()

    concat.to_parquet(path_final_matches, index=False)

    top_match = concat.drop_duplicates('qpub_original_line_number', keep='first')
    top_match.to_parquet(path_final_top_match, index=False)
