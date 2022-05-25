import awswrangler as wr

from dotenv import load_dotenv
load_dotenv()
import time
from retrying import retry
import pandas as pd

from helper.logger import logger

source = 'impression-data-bucket-v2'

destination = 'impression-data-bucket-v3'
s3_athena_database = 'impression'
s3_athena_database_table = 'impression_service'

logger.inform(job_id = '', s = "Starting v2 to v3 migration for impression service")
start = time.time()

dfs = wr.s3.read_parquet(
    path=f's3://{source}/',
    path_root=f's3://{source}/', 
    dataset=True, ignore_empty=True, 
    chunked=True,
    partition_filter = lambda x: True if x["insertion_date_month"] == "5" else False,
)

logger.inform(job_id = '', s = f"Entire data read in {time.time() - start} seconds. Starting to write data.")

athena_dtypes = {
    'screen_name': 'string',
    'user_id': 'string',
    'ct_profile_id': 'string',
    'asset_id': 'string',
    'asset_type': 'string',
    'asset_parent_id': 'string',
    'asset_parent_type': 'string',
    'price': 'string',
    'mrp': 'string',
    'action': 'string',
    'app_type': 'string',
    'date': 'timestamp',
    'entity_type': 'string',
    'vertical_rank': 'bigint',
    'horizontal_rank': 'bigint',
    'source': 'string',
    'is_product_oos': 'boolean',
    'catalogue_name': 'string',
    'cms_page_id': 'string',
    'linked_cms': 'string',
    'linked_cat': 'string',
    'linked_subcat': 'string',
    'insertion_date': 'timestamp',
    'app_code' : 'string',
    'app_version': 'string',
    'insertion_date_year': 'string',
    'insertion_date_month': 'string',
    'insertion_date_day': 'string'
}

@retry(wait_random_min=3000, wait_random_max=5000)
def save_to_s3(df: pd.DataFrame() = None) -> None:
    try:
        logger.inform(job_id = '', s = f"Migrating {df.shape[0]} records.")
        df.drop(columns = ['insertion_date_hour'], axis = 1, errors = 'ignore', inplace = True)
        df['app_version'] = df['app_version'].astype(str, copy=False, errors='ignore')
        df['insertion_date_year'] = df['insertion_date_year'].astype(str, copy=False, errors='ignore')
        df['insertion_date_month'] = df['insertion_date_month'].astype(str, copy=False, errors='ignore')
        df['insertion_date_day'] = df['insertion_date_day'].astype(str, copy=False, errors='ignore')
        wr.s3.to_parquet(
            df = df,
            path = f's3://{destination}',
            compression = 'snappy',
            mode='append',
            database=s3_athena_database,
            table=s3_athena_database_table,
            dtype=athena_dtypes,
            description = 'data coming from impression service v2',
            use_threads=False,
            concurrent_partitioning=False,
            dataset=True,
            partition_cols=['insertion_date_year', 'insertion_date_month', 'insertion_date_day'],
            schema_evolution=True,
        )
    except Exception as e:
        logger.err(job_id='', s = str(e))
        logger.info(jobid = '', s = "RETRYING")
        raise

for df in dfs:
    save_to_s3(df)


print("Completed Migration from v2 to v3, putting app to sleep to prevent pod from restating")
time.sleep(100000000000)