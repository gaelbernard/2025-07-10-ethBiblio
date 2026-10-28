'''
Convert the works of the downloaded OpenAlex snapshot (INPUT_DIR) into Parquet batches (OUTPUT_DIR)
containing only the fields needed to match the publication lists (step 2_matches).
Only works published between 2013 and 2024 are kept.
'''
from modules.match.ParquetBatchProcessor import ParquetBatchProcessor

INPUT_DIR = "snapshot_downloaded_2025_09_03"
OUTPUT_DIR = "snapshot_in_parquets_2025_10_15"
BATCH_SIZE = 100_000

processor = ParquetBatchProcessor(input_dir=INPUT_DIR, output_dir=OUTPUT_DIR, batch_size=BATCH_SIZE)
processor.process()
