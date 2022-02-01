import pandas as pd
import awswrangler as wr
from helper.logger import logger

def save_to_s3(df_list, db):
    s3_location = "s3://" + db['s3_bucket_name'] + "/" + db['source_type'] + "/" + db['db_name'] + "/"
    for dfl in df_list:
        file_name = s3_location + dfl['collection_name'] + "/"
        logger.info("Attempting to save " + file_name)
        if(dfl['df_collection'].shape[0] > 0):
            wr.s3.to_parquet(
                df = dfl['df_collection'],
                path = file_name,
                compression='snappy',
                dataset = True,
                partition_cols = ['parquet_format_date_year', 'parquet_format_date_month']
            )
            logger.info("Saved " + file_name + " with " + str(dfl['df_collection'].shape[0]) + " records.")
        else:
            logger.info("No insertions done.")
        
        for i in range(dfl['df_collection_update'].shape[0]):
            file_name_u = s3_location + dfl['collection_name'] + "/" + "parquet_format_date_year=" + dfl['df_collection_update'].iloc[i]['parquet_format_date_year'] + "/parquet_format_date_month=" + dfl['df_collection_update'].iloc[i]['parquet_format_date_month'] + "/"
            df_to_be_updated = wr.s3.read_parquet(
                path = file_name_u,
                dataset = True
            )
            df_to_be_updated[df_to_be_updated['_id'] == dfl['df_collection_update'].iloc[i]['_id']] = dfl['df_collection_update'].iloc[i]
            wr.s3.to_parquet(
                df = df_to_be_updated,
                mode = 'overwrite',
                path = file_name_u,
                compression = 'snappy',
                dataset = True,
            )
        logger.info(str(dfl['df_collection_update'].shape[0]) + " updations done for " + file_name)
