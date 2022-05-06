import psycopg2
import pandas as pd
from typing import List, Dict, Any
import datetime

from helper.logger import logger
from helper.util import utc_to_local
from helper.exceptions import *

class PgSQLSaver:
    def __init__(self, db_source: Dict[str, Any] = {}, db_destination: Dict[str, Any] = {}, unique_id: str = "") -> None:
        # s3_location is required as a staging area to push into PgSQL
        self.unique_id = unique_id
        if('username' not in db_destination.keys() or not db_destination['username']):
            db_destination['username'] = ''
        if('password' not in db_destination.keys() or not db_destination['password']):
            db_destination['password'] = ''
        self.conn = psycopg2.connect(
            host = db_destination['url'],
            database = db_destination['db_name'],
            user = db_destination['username'],
            password = db_destination['password']
        )
        self.schema = db_destination['schema'] if 'schema' in db_destination.keys() and db_destination['schema'] else db_source['source_type'] + "_" + db_source['db_name'] + "_dms"
        self.name_ = ""
        self.table_list = []



    def pgsql_create_table(self, df: pd.DataFrame = None, table: str = None, schema: str = None, dtypes: Dict[str, str] = {}, primary_keys: List[str] = [], varchar_lengths: Dict[str, int] = {}, varchar_lengths_default: int = 512) -> None:
        if(df.empty):
            raise EmptyDataframe("Dataframe can not be empty.")
        table_name = schema + "." + table if schema and len(schema) > 0 else table
        cols_def = ""
        for col in df.columns.to_list():
            cols_def = cols_def + "\n" + col + " "
            if(col not in dtypes.keys() or dtypes[col] == 'string'):
                cols_def = cols_def + "VARCHAR({0})".format(varchar_lengths[col] if col in varchar_lengths.keys() and varchar_lengths[col] else varchar_lengths_default)
            elif(dtypes[col] == 'timestamp'):
                cols_def = cols_def + "TIMESTAMP"
            elif(dtypes[col] == 'boolean'):
                cols_def = cols_def + "BOOLEAN"
            elif(dtypes[col] == 'bigint'):
                cols_def = cols_def + "BIGINT"
            elif(dtypes[col] == 'double'):
                cols_def = cols_def + "DOUBLE PRECISION"
            if(len(primary_keys) > 0 and primary_keys[0] == col):
                cols_def = cols_def + " PRIMARY KEY"
            cols_def = cols_def + ","
        cols_def = cols_def[:-1]

        sql_query = "CREATE TABLE IF NOT EXISTS {0} ({1}\n);".format(table_name, cols_def)
        with self.conn.cursor() as curs:
            curs.execute(sql_query)
            self.conn.commit()



    def pgsql_insert_records(self, df: pd.DataFrame = None, table: str = None, schema: str = None, dtypes: Dict[str, str] = {}, primary_keys: List[str] = [], varchar_lengths: Dict[str, int] = {}, varchar_lengths_default: int = 512) -> None:
        if(df.empty):
            raise EmptyDataframe("Dataframe can not be empty.")
        try:
            self.pgsql_create_table(df=df, table=table, schema=schema, dtypes=dtypes, primary_keys=primary_keys, varchar_lengths=varchar_lengths, varchar_lengths_default=varchar_lengths_default)
            table_name = schema + "." + table if schema and len(schema) > 0 else table
            col_names = ""
            list_cols = df.columns.to_list()
            for col in list_cols:
                col_names = col_names + col + ", "

            col_names = col_names[:-2]
            for key, val in dtypes.items():
                if(val == 'timestamp'):
                    df[key] = df[key].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f') if not pd.isnull(x) else '')
            
            cols_def = ""
            for index, row in df.iterrows():
                row_def = "("
                for col in list_cols:
                    if(pd.isna(row[col])):
                        row_def += "NULL"
                    elif(col not in dtypes.keys() or dtypes[col] == 'string'):
                        row_def += "\'{0}\'".format(row[col].replace("'", "''"))
                    elif(dtypes[col] == 'timestamp'):
                        if(len(row[col]) > 0):
                            row_def += "CAST(\'{0}\' AS TIMESTAMP)".format(row[col])
                        else:
                            row_def += "NULL"
                    elif(dtypes[col] == 'boolean'):
                        if(dtypes[col]):
                            row_def += "True"
                        else:
                            row_def += "False"
                    elif(dtypes[col] == 'bigint' or dtypes[col] == 'double'):
                        row_def += '{0}'.format(row[col])
                    row_def += ", "
                row_def = row_def[:-2] + "),\n"
                cols_def += row_def
            cols_def = cols_def[:-2]

            sql_query = "INSERT INTO {0}({1}) VALUES \n{2};".format(table_name, col_names, cols_def)
            with self.conn.cursor() as curs:
                curs.execute(sql_query)
                self.conn.commit()
        except Exception as e:
            raise Exception("Unable to insert records in table.")




    def pgsql_upsert_records(self, df: pd.DataFrame = None, table: str = None, schema: str = None, dtypes: Dict[str, str] = {}, primary_keys: List[str] = [], varchar_lengths: Dict[str, int] = {}, varchar_lengths_default: int = 512) -> None:
        if(df.empty):
            raise EmptyDataframe("Dataframe can not be empty.")
        try:
            self.pgsql_create_table(df=df, table=table, schema=schema, dtypes=dtypes, primary_keys=primary_keys, varchar_lengths=varchar_lengths, varchar_lengths_default=varchar_lengths_default)
            table_name = schema + "." + table if schema and len(schema) > 0 else table
            col_names = ""
            list_cols = df.columns.to_list()
            for col in list_cols:
                col_names = col_names + col + ", "

            col_names = col_names[:-2]
            for key, val in dtypes.items():
                if(val == 'timestamp'):
                    df[key] = df[key].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f') if not pd.isnull(x) else '')

            cols_def = ""
            for index, row in df.iterrows():
                row_def = "("
                for col in list_cols:
                    if(pd.isna(row[col])):
                        row_def += "NULL"
                    elif(col not in dtypes.keys() or dtypes[col] == 'string'):
                        row_def += "\'{0}\'".format(row[col].replace("'", "''"))
                    elif(dtypes[col] == 'timestamp'):
                        if(len(row[col]) > 0):
                            row_def += "CAST(\'{0}\' AS TIMESTAMP)".format(row[col])
                        else:
                            row_def += "NULL"
                    elif(dtypes[col] == 'boolean'):
                        if(dtypes[col]):
                            row_def += "True"
                        else:
                            row_def += "False"
                    elif(dtypes[col] == 'bigint' or dtypes[col] == 'double'):
                        row_def += '{0}'.format(row[col])
                    row_def += ", "
                row_def = row_def[:-2] + "),\n"
                cols_def += row_def
            cols_def = cols_def[:-2]

            up_query = ""
            for col in list_cols:
                if(col != primary_keys[0]):
                    up_query += "{0} = excluded.{0},\n".format(col)
            if(len(up_query)>2):
                up_query = up_query[:-2]

            sql_query = "INSERT INTO {0}({1}) VALUES \n{2}\n ON CONFLICT ({3}) DO UPDATE SET {4};".format(table_name, col_names, cols_def, primary_keys[0], up_query)
            with self.conn.cursor() as curs:
                curs.execute(sql_query)
                self.conn.commit()
        except Exception as e:
            raise Exception("Unable to insert records in table.")



    def inform(self, message: str = "") -> None:
        logger.inform(job_id=self.unique_id, s= (self.unique_id + ": " + message))
    
    def warn(self, message: str = "") -> None:
        logger.warn(job_id= self.unique_id, s=(self.unique_id + ": " + message))

    def save(self, processed_data: Dict[str, Any] = None, primary_keys: List[str] = None) -> None:
        if(not self.name_ or not(self.name_ == processed_data['name'])):
            self.table_list.extend(processed_data['name']) 
        self.name_ = processed_data['name']
        
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
            self.pgsql_insert_records(
                df = processed_data['df_insert'],
                table = self.name_,
                schema = self.schema,
                dtypes = processed_data['dtypes'],
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
            self.pgsql_upsert_records(
                df = processed_data['df_update'],
                table = self.name_,
                schema = self.schema,
                dtypes = processed_data['dtypes'],
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
        self.inform("Deleted " + table_name + " from PgSQL schema " + self.schema)


    def get_n_cols(self, table_name: str = None) -> int:
        query = 'SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = \'{0}\' AND table_name = \'{1}\''.format(self.schema, table_name)
        self.inform(query)
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            df = pd.DataFrame(cursor.fetchall(), columns=['count'])
        return df.iloc[0][0]


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
        ## Expire function is called only when Mode = Dumping
        ## i.e. the saved data will have a migration_snapshot_date column
        ## We just have to query using that column to delete old data
        delete_before_date_str = delete_before_date.strftime('%Y-%m-%d %H:%M:%S')
        for table_name in self.table_list:
            query = "DELETE FROM " + self.schema + "." + table_name + " WHERE migration_snapshot_date <= " + delete_before_date_str + ";"
            with self.conn.cursor() as cursor:
                cursor.execute(query)
    
    def close(self):
        self.conn.close()


'''

    INSERT INTO TABLE VALUES 
    (E'Haldiram'S')

'''