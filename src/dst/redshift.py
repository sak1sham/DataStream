import awswrangler as wr
import redshift_connector
from typing import List, Dict, Any
import datetime

from helper.logger import logger
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
        self.schema = db_destination['schema'] if 'schema' in db_destination.keys() and db_destination['schema'] else (db_source['source_type'] + "_" + db_source['db_name'] + "_dms").replace('-', '_').replace('.', '_')
        self.is_small_data = is_small_data
        self.name_ = ""
        self.table_list = []

    def inform(self, message: str = "") -> None:
        logger.inform(job_id=self.unique_id, s= (self.unique_id + ": " + message))
    
    def warn(self, message: str = "") -> None:
        logger.warn(job_id= self.unique_id, s=(self.unique_id + ": " + message))

    def err(self, message: str = "") -> None:
        logger.warn(job_id= self.unique_id, s=(self.unique_id + ": " + message))


    def save(self, processed_data: Dict[str, Any] = None, primary_keys: List[str] = None) -> None:
        if(not self.name_ or not(self.name_ == processed_data['name'])):
            self.table_list.extend(processed_data['name']) 
        self.name_ = processed_data['name']
        file_name = self.s3_location + self.name_ + "/"
        
        if 'filename' in processed_data:
            file_name += str(processed_data['filename']) + "/"
        
        varchar_lengths = processed_data['lob_fields_length'] if 'lob_fields_length' in processed_data else {}
        
        if('col_rename' in processed_data and processed_data['col_rename']):
            for key, val in processed_data['col_rename'].items():
                if(key in varchar_lengths.keys()):
                    varchar_lengths[val] = varchar_lengths[key]
                    varchar_lengths.pop(key)
        
        if('df_insert' in processed_data and processed_data['df_insert'].shape[0] > 0):
            if('col_rename' in processed_data and processed_data['col_rename']):
                processed_data['df_insert'].rename(columns = processed_data['col_rename'], inplace = True)
            self.inform(message=("Attempting to insert " + str(processed_data['df_insert'].memory_usage(index=True).sum()) + " bytes."))
            if(self.is_small_data):
                wr.redshift.to_sql(
                    df = processed_data['df_insert'],
                    con = self.conn,
                    schema = self.schema,
                    table = self.name_,
                    mode = "append",
                    primary_keys = primary_keys,
                    varchar_lengths = varchar_lengths,
                    varchar_lengths_default = 512
                )
            else:
                wr.redshift.copy(
                    df = processed_data['df_insert'],
                    con = self.conn,
                    path = file_name,
                    schema = self.schema,
                    table = self.name_,
                    mode = "append",
                    primary_keys = primary_keys,
                    varchar_lengths = varchar_lengths,
                    varchar_lengths_default = 512
                )
            self.inform(message=("Inserted " + str(processed_data['df_insert'].shape[0]) + " records."))    
        
        if('df_update' in processed_data and processed_data['df_update'].shape[0] > 0):
            self.inform(message=("Attempting to update " + str(processed_data['df_update'].memory_usage(index=True).sum()) + " bytes."))
            if('col_rename' in processed_data and processed_data['col_rename']):
                processed_data['df_update'].rename(columns = processed_data['col_rename'], inplace = True)
            # is_dump = False, and primary_keys will be present.
            if(self.is_small_data):
                wr.redshift.to_sql(
                    df = processed_data['df_update'],
                    con = self.conn,
                    schema = self.schema,
                    table = self.name_,
                    mode = "upsert",
                    primary_keys = primary_keys,
                    varchar_lengths = varchar_lengths,
                    varchar_lengths_default = 512
                )
            else:
                wr.redshift.copy(
                    df = processed_data['df_update'],
                    path = file_name,
                    con = self.conn,
                    schema = self.schema,
                    table = self.name_,
                    mode = "upsert",
                    primary_keys = primary_keys,
                    varchar_lengths = varchar_lengths,
                    varchar_lengths_default = 512
                )
            self.inform(message=(str(processed_data['df_update'].shape[0]) + " updations done."))


    def delete_table(self, table_name: str = None) -> None:
        query = "DROP TABLE " + self.schema + "." + table_name + ";"
        self.inform(query)
        with self.conn.cursor() as cursor:
            cursor.execute(query)
        self.inform("Deleted " + table_name + " from Redshift schema " + self.schema)


    def get_n_cols(self, table_name: str = None) -> int:
        query = 'SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = \'{0}\' AND table_name = \'{1}\''.format(self.schema, table_name)
        self.inform(query)
        df = wr.redshift.read_sql_query(
            sql = query,
            con = self.conn
        )
        return df.iloc[0][0]


    def is_exists(self, table_name: str = None) -> bool:
        try:
            sql_query = f'SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \'{self.schema}\' AND TABLE_NAME = \'{table_name}\';'
            self.inform(sql_query)
            df = wr.redshift.read_sql_query(
                sql = sql_query,
                con = self.conn
            )
            return df.shape[0] > 0
        except Exception as e:
            self.err("Unable to test if the table is present previously at destination.")
            self.err(str(e))
            raise


    def count_n_records(self, table_name: str = None) -> int:
        try:
            if(self.is_exists(table_name=table_name)):
                sql_query = f'SELECT COUNT(*) as count FROM {self.schema}.{table_name}'
                self.inform(sql_query)
                df = wr.redshift.read_sql_query(
                    sql = sql_query,
                    con = self.conn
                )
                return df.iloc[0][0]
            else:
                return 0
        except Exception as e:
            self.err("Unable to fetch the number of records previously at destination.")
            self.err(str(e))
            raise

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
        self.inform(message=("Trying to expire data which was modified on or before " + delete_before_date.strftime('%Y/%m/%d')))
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