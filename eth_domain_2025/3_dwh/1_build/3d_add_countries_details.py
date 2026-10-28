import duckdb
from pathlib import Path
import requests
import pandas as pd

from utils import DB_FILE

DB_PATH = Path(DB_FILE)

con = duckdb.connect(str(DB_PATH))
con.execute("CREATE SCHEMA IF NOT EXISTS augmented;")

# Just to see what you already have
dim_country = con.execute("""
    SELECT *
    FROM core.dim_country
""").df()
print(dim_country.head())

countries = []
cursor = '*'

while True:
    url = "https://api.openalex.org/countries"
    params = {"cursor": cursor, "per-page": 200}
    data = requests.get(url, params=params).json()

    for r in data["results"]:
        rec = {}
        rec["code"] = r["id"].replace("https://openalex.org/countries/", "")
        rec["display_name"] = r["display_name"]
        rec["iso"] = r["ids"].get("iso")
        rec["wikidata"] = r["ids"].get("wikidata")
        rec["wikipedia"] = r["ids"].get("wikipedia")

        if r.get("continent"):
            rec["continent_id"] = r["continent"]["id"].replace("https://openalex.org/continents/", "")
            rec["continent_name"] = r["continent"]["display_name"]
        else:
            rec["continent_id"] = None
            rec["continent_name"] = None

        countries.append(rec)

    cursor = data["meta"].get("next_cursor")
    if cursor is None:
        break

countries_df = pd.DataFrame(countries)
print(countries_df.head())

con.register("countries_df", countries_df)

con.execute("""
CREATE OR REPLACE TABLE augmented.openalex_countries AS
SELECT
    code,
    display_name,
    iso,
    wikidata,
    wikipedia,
    continent_id,
    continent_name
FROM countries_df;
""")