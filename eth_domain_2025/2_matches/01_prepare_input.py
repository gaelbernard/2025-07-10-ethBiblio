import ast

import pandas as pd

from modules.match.PublicationList import PublicationList

'''
This code prepares the 6 publication lists provided by the libraries (not included in this repository).
Each list is placed in {institution}/input/ and standardised into {institution}/input/qpub.parquet.
'''
# ETH Zürich
print ('\n\n\n** Processing ETHZ **')
df = pd.read_csv('ethz/input/20250919_ETHZ_selfeval-delivery_rc-opal-together_items.csv', dtype={'dois':str, 'openalex_id':str})
df['publication_type'] = ''
df['authors_all'] = df['authors']
df['openalex_id'] = df['openalex_id'].str.replace('W', '', regex=False).astype(float)
PublicationList(df, 'ethz/input/qpub.parquet')

# EPFL
print ('\n\n\n** Processing EPFL **')
df = pd.read_csv('epfl/input/2014-2023_eth_bibliometric_dataset_epfl_final.csv')
df = df.rename(columns={'doi': 'dois'})
# Some journals were returned as lists, some not so we need to uniformize this
# Also, only 11 records have more than 1 journal, so we keep only the first one
df['journal'] = df['journal'].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) and x.startswith('[') else [x])
df['journal'] = df['journal'].str[0]

df['openalex_id'] = df['openalex_id'].astype(str).str.replace('W', '', regex=False).astype(float)
PublicationList(df, 'epfl/input/qpub.parquet')

# EAWAG
print ('\n\n\n** Processing EAWAG **')
df = pd.read_csv('eawag/input/Eawag_2013-2024_final.csv', sep=';')
df = df.rename(columns={'authors': 'authors_all'})
df['openalex_id'] = df['openalex_id'].astype(str).str.lower().str.replace('w', '', regex=False).astype(float)
PublicationList(df, 'eawag/input/qpub.parquet')

# EMPA
print ('\n\n\n** Processing EMPA **')
df = pd.read_csv('empa/input/Empa_2013-2024_final.csv', sep=';')
df = df.rename(columns={'authors': 'authors_all'})
df['openalex_id'] = df['openalex_id'].astype(str).str.lower().str.replace('w', '', regex=False).astype(float)
PublicationList(df, 'empa/input/qpub.parquet')

# PSI
print ('\n\n\n** Processing PSI **')
df = pd.read_csv('psi/input/PSI_2013-2024_final.csv', sep=';')
df = df.rename(columns={'authors': 'authors_all'})
df['openalex_id'] = df['openalex_id'].astype(str).str.lower().str.replace('w', '', regex=False).astype(float)
PublicationList(df, 'psi/input/qpub.parquet')

# WSL
print ('\n\n\n** Processing WSL **')
df = pd.read_csv('wsl/input/WSL_2013-2024_final.csv', sep=';')
df = df.rename(columns={'authors': 'authors_all'})
# The export ends with empty rows
df = df.dropna(subset=['internal_id'])
df['openalex_id'] = df['openalex_id'].astype(str).str.lower().str.replace('w', '', regex=False).astype(float)
PublicationList(df, 'wsl/input/qpub.parquet')
