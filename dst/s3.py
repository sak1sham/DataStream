import pandas as pd
import awswrangler as wr
from helper.logger import logger

def save_to_s3(df_list, db):
    s3_location = "s3://" + db['s3_bucket_name'] + "/" + db['source_type'] + "/" + db['db_name'] + "/"
    for dfl in df_list:
        file_name = s3_location + dfl['collection_name'] + "/"
        logger.info("Attempting to save " + file_name)
        wr.s3.to_parquet(
            df = dfl['df_collection'],
            path = file_name,
            compression='snappy',
            dataset = True,
            partition_cols = ['parquet_format_date_year', 'parquet_format_date_month']
        )
        logger.info("Saved " + file_name)