from re import T
import awswrangler as wr
import redshift_connector

from helper.logger import logger

from typing import List, Dict, Any
import datetime
from helper.util import utc_to_local

class RedshiftSaver:
    def __init__(self, db_source: Dict[str, Any] = {}, db_destination: Dict[str, Any] = {}, unique_id: str = "", is_small_data: bool = False) -> None:
        # s3_location is required as a staging area to push into redshift
        self.s3_location = "s3://" + db_destination['s3_bucket_name'] + "/Redshift/" + db_source['source_type'] + "/" + db_source['db_name'] + "/"
        self.unique_id = unique_id
        self.conn = redshift_connector.connect(
            host = db_destination['host'],
            database = db_destination['database'],
            user = db_destination['user'],
            password = db_destination['password']
        )
        self.schema = db_destination['schema']
        self.is_small_data = is_small_data
        self.name_ = ""
        self.table_list = []

    def inform(self, message: str = "", save: bool = False) -> None:
        logger.inform(job_id=self.unique_id, s= (self.unique_id + ": " + message), save=save)
    
    def warn(self, message: str = "") -> None:
        logger.warn(job_id= self.unique_id, s=(self.unique_id + ": " + message))

    def save(self, processed_data: Dict[str, Any] = None, primary_keys: List[str] = None) -> None:
        if(not self.name_ or not(self.name_ == processed_data['name'])):
            self.table_list.extend(processed_data['name']) 
        self.name_ = processed_data['name']
        file_name = self.s3_location + self.name_ + "/"
        self.inform(message=("Attempting to insert " + str(processed_data['df_insert'].memory_usage(index=True).sum()) + " bytes."), save=True)
        if(processed_data['df_insert'].shape[0] > 0):
            if(self.is_small_data):
                wr.redshift.to_sql(
                    df = processed_data['df_insert'],
                    con = self.conn,
                    schema = self.schema,
                    table = self.name_,
                    mode = "append",
                    primary_keys = primary_keys
                )
            else:
                wr.redshift.copy(
                    df = processed_data['df_insert'],
                    con = self.conn,
                    path = file_name,
                    schema = self.schema,
                    table = self.name_,
                    mode = "append",
                    primary_keys = primary_keys
                )
        self.inform(message=("Inserted " + str(processed_data['df_insert'].shape[0]) + " records."), save=True)
        self.inform(message=("Attempting to update " + str(processed_data['df_update'].memory_usage(index=True).sum()) + " bytes."), save=True)
        if(processed_data['df_update'].shape[0] > 0):
            # is_dump = False, and primary_keys will be present.
            if(self.is_small_data):
                wr.redshift.to_sql(
                    df = processed_data['df_update'],
                    con = self.conn,
                    schema = self.schema,
                    table = self.name_,
                    mode = "upsert",
                    primary_keys = primary_keys
                )
            else:
                wr.redshift.copy(
                    df = processed_data['df_update'],
                    path = file_name,
                    con = self.conn,
                    schema = self.schema,
                    table = self.name_,
                    mode = "upsert",
                    primary_keys = primary_keys
                )
        self.inform(message=(str(processed_data['df_update'].shape[0]) + " updations done."), save=True)
    
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
        self.inform(message=("Trying to expire data which was modified on or before " + delete_before_date.strftime('%Y/%m/%d')), save=True)
        ## Expire function is called only when is_dump = True
        ## i.e. the saved data will have a migration_snapshot_date column
        ## We just have to query using that column to delete old data
        delete_before_date_str = delete_before_date.strftime('%Y-%m-%d %H:%M:%S')
        for table_name in self.table_list:
            query = "DELETE FROM " + self.schema + "." + table_name + " WHERE migration_snapshot_date <= " + delete_before_date_str + ";"
            with self.conn.cursor() as cursor:
                cursor.execute(query)
    
    def close(self):
        self.conn.close()