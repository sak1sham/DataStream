import awswrangler as wr
import boto3
from urllib.parse import urlparse

from typing import List, Dict, Any
import datetime
from dotenv import load_dotenv
load_dotenv()

from helper.logger import logger
from helper.util import convert_heads_to_lowercase
from helper.util import utc_to_local, df_update_records
from dst.s3.utils import insert_s3, update_s3_file, get_file_df

class s3Saver:
    def __init__(self, db_source: Dict[str, Any] = {}, db_destination: Dict[str, Any] = {}, c_partition: List[str] = [], unique_id: str = "") -> None:
        self.source_type = db_source['source_type']
        if(self.source_type == 'pgsql'):
            self.source_type = 'sql'
        self.s3_location = f"s3://{db_destination['s3_bucket_name']}/{self.source_type}/{db_source['db_name']}/"
        self.s3_suffix = "" if 's3_suffix' not in db_destination.keys() or not db_destination['s3_suffix'] else db_destination['s3_suffix']
        self.partition_cols = convert_heads_to_lowercase(c_partition)
        self.unique_id = unique_id
        self.name_ = ""
        self.table_list = []
        self.database = (f"{self.source_type}_{db_source['db_name']}").replace(".", "_").replace("-", "_")
        self.description = f"Data migrated from {self.database}"

    def inform(self, message: str = "") -> None:
        logger.inform(s = f"{self.unique_id}: {message}")

    def warn(self, message: str = "") -> None:
        logger.warn(s = f"{self.unique_id}: {message}")

    def err(self, message: str = "") -> None:
        logger.err(s = f"{self.unique_id}: {message}")

    def save(self, processed_data: Dict[str, Any] = None, c_partition: List[str] = [], primary_keys: List[str] = None) -> None:
        if(c_partition and len(c_partition) > 0):
            self.partition_cols = convert_heads_to_lowercase(c_partition)
        if(not self.name_ or not(self.name_ == processed_data['name'])):
            self.table_list.extend(processed_data['name'])
        self.name_ = processed_data['name']
        file_name = f"{self.s3_location}{processed_data['name']}{self.s3_suffix}/"

        if(processed_data['df_insert'].shape[0] > 0):
            self.inform(message = f"Attempting to insert {str(processed_data['df_insert'].memory_usage(index=True).sum())} bytes.")
            processed_data['df_insert'] = convert_heads_to_lowercase(processed_data['df_insert'])
            processed_data['dtypes'] = convert_heads_to_lowercase(processed_data["dtypes"])
            insert_s3(
                df = processed_data['df_insert'],
                path = file_name,
                compression='snappy',
                mode = 'append',
                database = self.database,
                table = f"{self.name_}{self.s3_suffix}",
                dtype = processed_data['dtypes'],
                description = self.description,
                dataset = True,
                partition_cols = self.partition_cols,
                schema_evolution = True,
                job_id = self.unique_id
            )
            self.inform(message = f"Inserted {str(processed_data['df_insert'].shape[0])} records.")
        
        not_found = []
        n_updations = processed_data['df_update'].shape[0]
        if(n_updations > 0):
            self.inform(message = f"Attempting to update {str(n_updations)} records or {str(processed_data['df_update'].memory_usage(index=True).sum())} bytes.")
            processed_data['df_update'] = convert_heads_to_lowercase(processed_data['df_update'])
            dfs_u = [processed_data['df_update'].copy()]
            if(self.partition_cols and len(self.partition_cols) > 0):
                print(processed_data['df_update'].groupby(self.partition_cols).size())
                dfs_u = [x for _, x in processed_data['df_update'].groupby(self.partition_cols)]
            ## Now all the records which are of same partition are grouped together, and will be updated in the same run
            for df_u in dfs_u:
                file_name_u = f"{self.s3_location}{processed_data['name']}{self.s3_suffix}"
                if(self.partition_cols):
                    for x in self.partition_cols:
                        file_name_u = f"{file_name_u}/{x}={str(df_u.iloc[0][x])}"
                    df_u.drop(self.partition_cols, axis=1, inplace=True)
                ## Now, all records within df_u are found within this same location
                prev_files = wr.s3.list_objects(file_name_u)
                self.inform(message = f"Found {len(prev_files)} files while updating: {df_u.shape[0]}/{n_updations} records")
                for file_ in prev_files:
                    s3 = boto3.client('s3') 
                    file_o = urlparse(file_, allow_fragments=False)
                    bucket_name_read = file_o.netloc
                    file_name_read = file_o.path[1:]
                    df_to_be_updated = get_file_df(s3, bucket_name_read, file_name_read)
                    df_to_be_updated, modified, df_u = df_update_records(df=df_to_be_updated, df_u=df_u, primary_key=primary_keys[0])
                    if(modified):
                        update_s3_file(
                            df = df_to_be_updated,
                            path = file_,
                            compression = 'snappy',
                            job_id = self.unique_id
                        )
                        if(df_u.shape[0] == 0):
                            ## This partition-batch is complete now. Go and handle the next set of partition-batches to be updated
                            break
                if(df_u.shape[0] > 0):
                    self.warn(f"Not all records could be updated, because {df_u.shape[0]} records could not be found. Will be trying to insert them.")
                    not_found.extend(df_u[primary_keys[0]].tolist())
            self.inform(message = f"{n_updations-len(not_found)} updations done.")
            
            if(len(not_found) > 0):
                file_name = f"{self.s3_location}{processed_data['name']}{self.s3_suffix}/"
                records_update_insert = processed_data["df_update"][processed_data["df_update"][primary_keys[0]].isin(not_found)]
                records_update_insert = convert_heads_to_lowercase(records_update_insert)
                self.inform(message = f"Attempting to insert {records_update_insert.memory_usage(index=True).sum()} bytes.")
                processed_data['dtypes'] = convert_heads_to_lowercase(processed_data["dtypes"])
                insert_s3(
                    df = records_update_insert,
                    path = file_name,
                    compression='snappy',
                    mode = 'append',
                    database = self.database,
                    table = f"{self.name_}{self.s3_suffix}",
                    dtype = processed_data['dtypes'],
                    description = self.description,
                    dataset = True,
                    partition_cols = self.partition_cols,
                    schema_evolution = True,
                    job_id = self.unique_id
                )
                self.inform(message = f"Inserted {records_update_insert.shape[0]} records which were not present before.")



    def expire(self, expiry: Dict[str, int], tz: Any = None) -> None:
        today_ = datetime.datetime.utcnow()
        if(tz):
            today_ = utc_to_local(today_, tz)
        days = 0
        hours = 0
        if('days' in expiry.keys()):
            days = expiry['days']
        if('hours' in expiry.keys()):
            hours = expiry['hours']
        delete_before_date = today_ - datetime.timedelta(days=days, hours=hours)
        self.inform(message = f"Trying to expire data which was modified on or before {delete_before_date.strftime('%Y/%m/%d')}")
        for table_name in self.table_list:
            wr.s3.delete_objects(
                path = f"{self.s3_location}{table_name}/",
                last_modified_end = delete_before_date
            )


    def is_exists(self, table_name: str = None) -> bool:
        try:
            sql_query = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \'{self.database}\' AND TABLE_NAME = \'{table_name}\';"
            self.inform(sql_query)
            df = wr.athena.read_sql_query(sql = sql_query, database = self.database)
            return df.shape[0] > 0
        except Exception as e:
            self.err("Unable to check the presence of the table at destination.")
            raise

    def count_n_records(self, table_name: str = None) -> int:
        try:
            if(self.is_exists(table_name=table_name)):
                sql_query = f"SELECT COUNT(*) as count FROM {table_name};"
                self.inform(sql_query)
                df = wr.athena.read_sql_query(sql = sql_query, database = self.database)
                return df.iloc[0][0]
            else:
                return 0
        except Exception as e:
            self.err("Unable to fetch the number of records previously at destination.")
            raise


    def close(self):
        # This function is required here to make it consistent with redshift connection closing counterpart
        return