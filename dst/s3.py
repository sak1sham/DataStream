import awswrangler as wr

from helper.logging import logger

from typing import List, Dict, Any
import datetime
from helper.util import utc_to_local, df_update_records

from dotenv import load_dotenv
load_dotenv()

class s3Saver:
    def __init__(self, db_source: Dict[str, Any] = {}, db_destination: Dict[str, Any] = {}, c_partition: List[str] = [], unique_id: str = "") -> None:
        self.s3_location = "s3://" + db_destination['s3_bucket_name'] + "/" + db_source['source_type'] + "/" + db_source['db_name'] + "/"
        self.partition_cols = c_partition
        self.unique_id = unique_id
        self.name_ = ""
        self.table_list = []
        self.database = (db_source["source_type"] + "_" + db_source["db_name"]).replace(".", "_").replace("-", "_")
        self.description = "Data migrated from " + self.database



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
                mode = 'append',
                database = self.database,
                table = self.name_,
                dtype = processed_data['dtypes'],
                description = self.description,
                dataset = True,
                partition_cols = self.partition_cols,
                schema_evolution = True,
            )
        self.inform("Inserted " + str(processed_data['df_insert'].shape[0]) + " records.")

        n_updations = processed_data['df_update'].shape[0]
        self.inform("Attempting to update " + str(n_updations) + " records or " + str(processed_data['df_update'].memory_usage(index=True).sum()) + " bytes.")
        if(n_updations > 0):
            print(processed_data['df_update'].groupby(self.partition_cols).size())
            dfs_u = [x for _, x in processed_data['df_update'].groupby(self.partition_cols)]
            ## Now all the records which are of same partition are grouped together, and will be updated in the same run
            for df_u in dfs_u:
                file_name_u = self.s3_location + processed_data['name'] + "/"
                for x in self.partition_cols:
                    file_name_u = file_name_u + x + "=" + str(processed_data['df_update'].iloc[0][x]) + "/"
                ## Now, all records within df_u are found within this same location
                df_u.drop(self.partition_cols, axis=1, inplace=True)
                prev_files = wr.s3.list_objects(file_name_u)
                self.inform("Found all files while updating: " + str(df_u.shape[0]) + " records out of " + str(n_updations))
                for file_ in prev_files:
                    df_to_be_updated = wr.s3.read_parquet(
                        path = [file_],
                    )
                    df_to_be_updated, modified, df_u = df_update_records(df=df_to_be_updated, df_u=df_u, primary_key=primary_keys[0])
                    if(modified):
                        wr.s3.to_parquet(
                            df = df_to_be_updated,
                            path = file_,
                            compression = 'snappy',
                        )
                    if(df_u.shape[0] == 0):
                        ## This one is complete now. Go and handle the next set of records to be updated
                        break
        self.inform(str(n_updations) + " updations done.")



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