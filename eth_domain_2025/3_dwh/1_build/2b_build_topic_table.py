# python >=3.10
'''
Load the OpenAlex topic hierarchy (topic > subfield > field > domain) into core.topics_tree.
'''
import duckdb
import pandas as pd

from modules.openalex_snapshot.OpenAlexSnapshot import OpenAlexSnapshot
from utils import DB_FILE, SNAPSHOT_DIR

con = duckdb.connect(str(DB_FILE))

snap = OpenAlexSnapshot(local_dir=str(SNAPSHOT_DIR))
snap.load_manifests()

output = []
for topic in snap.iterate("topics", test=False, show_progress=True):
    clean_topic = {}
    clean_topic['topic'] = int(topic['id'].replace('https://openalex.org/T', ''))
    clean_topic['topic_name'] = topic['display_name']
    clean_topic['topic_description'] = topic['description']
    clean_topic['topic_keywords'] = topic['keywords']
    clean_topic['subfield'] = int(topic['subfield']['id'].replace('https://openalex.org/subfields/', ''))
    clean_topic['subfield_name'] = topic['subfield']['display_name']
    clean_topic['field'] = int(topic['field']['id'].replace('https://openalex.org/fields/', ''))
    clean_topic['field_name'] = topic['field']['display_name']
    clean_topic['domain'] = int(topic['domain']['id'].replace('https://openalex.org/domains/', ''))
    clean_topic['domain_name'] = topic['domain']['display_name']
    output.append(clean_topic)
output = pd.DataFrame(output)

con.register("topics", output)

con.execute("""
    CREATE OR REPLACE TABLE core.topics_tree AS
    SELECT *
    FROM topics;
""")

con.execute("""
CREATE INDEX IF NOT EXISTS idx_topic_tree_subfield
  ON core.topics_tree(subfield);
""")

con.execute("CHECKPOINT;")
con.close()
print(f"✓ core.topics_tree built ({len(output):,} topics)")
