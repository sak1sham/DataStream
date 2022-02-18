import awswrangler as wr
import redshift_connector

import logging
logging.getLogger().setLevel(logging.INFO)

from typing import List, Dict, Any
import datetime
from helper.util import utc_to_local

class RedshiftSaver:
    def __init__(self, db_source: Dict[str, Any] = {}, db_destination: Dict[str, Any] = {}, unique_id: str = "") -> None:
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

    def inform(self, message: str = "") -> None:
        logging.info(self.unique_id + ": " + message)
    
    def warn(self, message: str = "") -> None:
        logging.warning(self.unique_id + ": " + message)

    def save(self, processed_data: Dict[str, Any] = None, primary_keys: List[str] = None) -> None:
        self.name_ = processed_data['name']
        file_name = self.s3_location + self.name_ + "/"
        self.inform("Attempting to insert " + str(processed_data['df_insert'].memory_usage(index=True).sum()) + " bytes.")
        if(processed_data['df_insert'].shape[0] > 0):
            wr.redshift.copy(
                df = processed_data['df_insert'],
                con = self.conn,
                path = file_name,
                schema = self.schema,
                table = self.name_,
                mode = "append",
                primary_keys = primary_keys
            )
        self.inform("Inserted " + str(processed_data['df_insert'].shape[0]) + " records.")
        self.inform("Attempting to update " + str(processed_data['df_update'].memory_usage(index=True).sum()) + " bytes.")    
        if(processed_data['df_update'].shape[0] > 0):
            wr.redshift.copy(
                df = processed_data['df_update'],
                path = file_name,
                con = self.conn,
                schema = self.schema,
                table = self.name_,
                mode = "upsert",
                primary_keys = primary_keys
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
        ## Expire function is called only when is_dump = True
        ## i.e. the saved data will have a migration_snapshot_date column
        ## We just have to query using that column to delete old data
        delete_before_date_str = delete_before_date.strftime('%Y-%m-%d %H:%M:%S')
        query = "DELETE FROM " + self.schema + "." + self.name_ + " WHERE migration_snapshot_date <= " + delete_before_date_str + ";"
        with self.conn.cursor() as cursor:
            cursor.execute(query)
    
    def close(self):
        self.conn.close()