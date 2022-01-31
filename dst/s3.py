import pandas as pd
import awswrangler as wr
from helper.logger import logger

def save_to_s3(df_list, db):
    for dfl in df_list:
        file_name = "s3://" + db['s3_bucket_name'] + "/" + db['source_type'] + "/" + db['db_name'] + "/" + dfl['collection_name'] + ".parquet"
        logger.info("Attempting to save " + file_name)
        wr.s3.to_parquet(
            df = dfl['df_collection'],
            path = file_name,
            compression='snappy',
            dataset = True,
            partition_cols = ['parquet_format_date_year', 'parquet_format_date_month']
        )
        logger.info("Saved " + file_name)

'''data = [['tom', 10], ['nick', 15], ['juli', 14]]
df = pd.DataFrame(data, columns = ['Name', 'Age'])

wr.s3.to_parquet(
    df=df,
    path="s3://migration-service-temp/my-file.parquet",
    compression='snappy',
    dataset = True,
    partition_cols = ['Age']
)
'''