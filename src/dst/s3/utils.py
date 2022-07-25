from retrying import retry
import pandas as pd
import awswrangler as wr
from typing import List, Dict, Any
import io

from helper.exceptions import EmptyDataframe
from helper.logger import logger

@retry(stop_max_attempt_number=10, wait_random_min=5000, wait_random_max=10000)
def insert_s3(df: pd.DataFrame = pd.DataFrame({}), path: str = None, compression: str = None, mode: str = None, database: str = None, table: str = None, dtype: Dict[str, str] = {}, description: str = None, dataset: bool = False, partition_cols: List[str] = [], schema_evolution: bool = True, job_id: str = ""):
    try:
        if(df is not None and df.shape[0] > 0):
            wr.s3.to_parquet(
                    df = df,
                    path = path,
                    compression = compression,
                    mode = mode,
                    database = database,
                    table = table,
                    dtype = dtype,
                    description = description,
                    dataset = dataset,
                    partition_cols = partition_cols,
                    schema_evolution = schema_evolution,
                )
        else:
            raise EmptyDataframe("Dataframe can not be empty.")
    except Exception as e:
        logger.err(s = str(e))
        raise


@retry(stop_max_attempt_number=10, wait_random_min=5000, wait_random_max=10000)
def update_s3_file(df: pd.DataFrame = pd.DataFrame({}), path: str = None, compression: str = None, job_id: str = ""):
    try:
        if(df is not None and df.shape[0] > 0):
            wr.s3.to_parquet(
                    df = df,
                    path = path,
                    compression = compression
                )
        else:
            raise EmptyDataframe("Dataframe can not be empty.")
    except Exception as e:
        logger.err(s = str(e))
        raise


@retry(stop_max_attempt_number=10, wait_random_min=5000, wait_random_max=10000)
def get_file_df(s3: Any = None, bucket_name_read: str = "", file_name_read: str = "") -> pd.DataFrame:
    try:
        obj_read = s3.get_object(Bucket=bucket_name_read, Key=file_name_read)
        df_to_be_updated = pd.read_parquet(io.BytesIO(obj_read['Body'].read()))
        return df_to_be_updated
    except Exception as e:
        logger.err(s = str(e))
        raise