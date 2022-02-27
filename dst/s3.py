import awswrangler as wr

from helper.logging import logger

from typing import List, Dict, Any
import datetime
from helper.util import utc_to_local, df_upsert

class s3Saver:
    def __init__(self, db_source: Dict[str, Any] = {}, db_destination: Dict[str, Any] = {}, c_partition: List[str] = [], unique_id: str = "") -> None:
        self.s3_location = "s3://" + db_destination['s3_bucket_name'] + "/" + db_source['source_type'] + "/" + db_source['db_name'] + "/"
        self.partition_cols = c_partition
        self.unique_id = unique_id
        self.name_ = ""
        self.table_list = []

    def inform(self, message: str = "") -> None:
        logger.inform(self.unique_id + ": " + message)
    
    def warn(self, message: str = "") -> None:
        logger.warn(self.unique_id + ": " + message)

    def save(self, processed_data: Dict[str, Any] = None, c_partition: List[str] = [], primary_keys: List[str] = None) -> None:
        if(c_partition and len(c_partition) > 0):
            self.partition_cols = c_partition
        if(not self.name_ or not(self.name_ == processed_data['name'])):
            self.table_list.extend(processed_data['name'])
        self.name_ = processed_data['name']
        file_name = self.s3_location + processed_data['name'] + "/"
        self.inform("Attempting to insert " + str(processed_data['df_insert'].memory_usage(index=True).sum()) + " bytes.")
        if(processed_data['df_insert'].shape[0] > 0):
            wr.s3.to_parquet(
                df = processed_data['df_insert'],
                path = file_name,
                compression='snappy',
                dataset = True,
                partition_cols = self.partition_cols,
                schema_evolution = True,
                database = "date-migration-database",
                table = processed_data['name'] + "_dms"
            )
        self.inform("Inserted " + str(processed_data['df_insert'].shape[0]) + " records.")

        self.inform("Attempting to update " + str(processed_data['df_update'].memory_usage(index=True).sum()) + " bytes.")
        file_name_u = self.s3_location + processed_data['name'] + "/"
        if(processed_data['df_update'].shape[0]):
            self.inform("There is something to update\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
        for i in range(processed_data['df_update'].shape[0]):
            for x in self.partition_cols:
                file_name_u = file_name_u + x + "=" + str(processed_data['df_update'].iloc[i][x]) + "/"
            self.inform(file_name_u)
            dfs_to_be_updated = wr.s3.read_parquet(
                path = file_name_u,
                dataset = True,
                chunked = 1000
            )
            for df_to_be_updated in dfs_to_be_updated:
                self.inform("Read the partition to be updated.")
                df_to_be_updated = df_upsert(df = df_to_be_updated, df_u = processed_data['df_update'].iloc[i:i+1], primary_key = primary_keys[0])
                wr.s3.to_parquet(
                    df = df_to_be_updated,
                    mode = 'overwrite_partitions',
                    path = file_name_u,
                    compression = 'snappy',
                    dataset = True,
                    schema_evolution = True,
                    database = "date-migration-database",
                    table = processed_data['name'] + "_dms"
                )
        self.inform(str(processed_data['df_update'].shape[0]) + " updations done.")
    
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
        self.inform("Trying to expire data which was modified on or before " + delete_before_date.strftime('%Y/%m/%d'))
        for table_name in self.table_list:
            wr.s3.delete_objects(
                path = self.s3_location + table_name + "/",
                last_modified_end = delete_before_date
            )
    
    def close(self):
        # This function is required here to make it consistent with redshift connection closing counterpart
        return