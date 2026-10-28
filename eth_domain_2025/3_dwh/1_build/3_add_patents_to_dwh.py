'''
Load the patent-paper pairs from Reliance on Science (see 1_data/patent/README.md) into augmented.pub_patent.
'''
from pathlib import Path
import duckdb, pandas as pd
from utils import DB_FILE

DB_PATH = Path(DB_FILE)
CSV_PATH = Path("../../1_data/patent/_patent_paper_pairs.csv")

patent = pd.read_csv(CSV_PATH)
patent["paperid"] = patent["paperid"].str.replace("W", "", regex=False).astype("int64")
patent = patent.rename(columns={"paperid": "work_id"})

con = duckdb.connect(str(DB_PATH))
con.execute("CREATE SCHEMA IF NOT EXISTS augmented;")
con.register("patent_df", patent)

# Create the table ONCE with correct types
con.execute("""
CREATE OR REPLACE TABLE augmented.pub_patent AS
SELECT
  CAST(work_id AS BIGINT) AS work_id,
  * EXCLUDE (work_id)
FROM patent_df
""")

# Now index it
con.execute("CREATE INDEX IF NOT EXISTS idx_pub_patent_work ON augmented.pub_patent(work_id);")
con.execute("CHECKPOINT;")
con.close()