import psycopg2
import pandas as pd
from typing import List, Dict, Any
import datetime
from retrying import retry
import pytz

from config.settings import settings
from helper.logger import logger
from helper.util import utc_to_local
from helper.exceptions import *


class PgSQLSaver:
    def __init__(self, db_source: Dict[str, Any] = {}, db_destination: Dict[str, Any] = {}, unique_id: str = "") -> None:
        self.source_type = db_source['source_type']
        if(self.source_type == 'pgsql'):
            self.source_type = 'sql'

        self.unique_id = unique_id
        if('username' not in db_destination.keys() or not db_destination['username']):
            db_destination['username'] = ''
        if('password' not in db_destination.keys() or not db_destination['password']):
            db_destination['password'] = ''
        self.db_destination = db_destination
        try:
            conn = psycopg2.connect(
                host=self.db_destination['url'],
                database=self.db_destination['db_name'],
                user=self.db_destination['username'],
                password=self.db_destination['password']
            )
            conn.close()
        except Exception as e:
            self.err("Unable to connect to destination.")
            raise
        else:
            self.inform("Successfully tested connection with destination db.")

        self.schema = db_destination['schema'] if 'schema' in db_destination.keys() and db_destination['schema'] else (f"{self.source_type}_{db_source['db_name']}_dms").replace('-', '_').replace('.', '_')
        self.name_ = ""
        self.table_list = []
        self.table_exists = None


    def create_schema_if_not_exists(self, schema: str = None) -> bool:
        conn = psycopg2.connect(
            host=self.db_destination['url'],
            database=self.db_destination['db_name'],
            user=self.db_destination['username'],
            password=self.db_destination['password']
        )
        with conn.cursor() as curs:
            curs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            conn.commit()
        conn.close()
        

    def check_table_exists(self, table: str = None, schema: str = None) -> bool:
        conn = psycopg2.connect(
            host=self.db_destination['url'],
            database=self.db_destination['db_name'],
            user=self.db_destination['username'],
            password=self.db_destination['password']
        )
        exists = False
        with conn.cursor() as curs:
            curs.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table}');")
            recs = curs.fetchall()
            exists = recs[0][0]
            conn.commit()
        conn.close()
        return exists


    def pgsql_create_table(self, df: pd.DataFrame = None, table: str = None, schema: str = None, dtypes: Dict[str, str] = {}, primary_keys: List[str] = [], varchar_length_source: Dict[str, int] = {}, logging_flag: bool = False, json_cols: List[str] = [], partition_col: str = None, indexes: List[str] = []) -> None:
        if(df.empty):
            raise EmptyDataframe("Dataframe can not be empty.")
        self.create_schema_if_not_exists(schema=schema)
        if(self.table_exists is None):
            # Not checked before
            self.table_exists = self.check_table_exists(table=self.name_, schema=self.schema)
            if(self.table_exists):
                self.inform("Table exists")
            else:
                self.inform("Table doesn't exist")

        if(self.table_exists):

            # Already checked the existence of table, and it exists
            return
        else:
            self.table_exists = True
            table_name = f"{schema}.{table}" if schema and len(schema) > 0 else table
            cols_def = ""
            for col in df.columns.to_list():
                cols_def = f"{cols_def} \n{col} "
                if(col not in dtypes.keys() or dtypes[col] == 'string' or dtypes[col] == 'str'):
                    if(col in json_cols):
                        cols_def = f"{cols_def} JSON"
                    elif(col in varchar_length_source.keys() and varchar_length_source[col]):
                        cols_def = cols_def + f" VARCHAR({varchar_length_source[col]})"
                    else:
                        cols_def = f"{cols_def} TEXT"
                elif(dtypes[col] == 'timestamp' or dtypes[col] == "datetime"):
                    cols_def = f"{cols_def} TIMESTAMP"
                elif(dtypes[col] == 'boolean' or dtypes[col] == "bool"):
                    cols_def = f"{cols_def} BOOLEAN"
                elif(dtypes[col] == 'bigint' or dtypes[col] == "int"):
                    cols_def = f"{cols_def} BIGINT"
                elif(dtypes[col] == 'double' or dtypes[col] == "float"):
                    cols_def = f"{cols_def} DOUBLE PRECISION"
                if(not logging_flag and len(primary_keys) > 0 and primary_keys[0] == col and not partition_col):
                    cols_def = f"{cols_def}  PRIMARY KEY"
                cols_def = f"{cols_def} ,"
            if(partition_col and len(primary_keys) > 0):
                cols_def = f"{cols_def} PRIMARY KEY ({primary_keys[0]}, {partition_col}),"
            cols_def = cols_def[:-1]

            logging_query = ""
            if(logging_flag):
                cols = ""
                for col in df.columns.to_list():
                    cols += f"{col}, "
                if(len(cols) > 0):
                    logging_query = f", UNIQUE ({cols[:-2]})"

            partition_string = ""
            if(partition_col):
                partition_string = f"\nPARTITION BY RANGE ({partition_col})"

            sql_query = f"CREATE TABLE {table_name} ({cols_def}\n{logging_query}){partition_string};"
            self.inform(sql_query)
            conn = psycopg2.connect(
                host=self.db_destination['url'],
                database=self.db_destination['db_name'],
                user=self.db_destination['username'],
                password=self.db_destination['password']
            )
            with conn.cursor() as curs:
                curs.execute(sql_query)
                conn.commit()

            if(partition_col):
                start_year = 2015
                end_year = 2030
                for year in range(start_year, end_year+1):
                    for month in range(1, 13):
                        sql_query = f"CREATE TABLE {table_name}_{year}_{month} PARTITION OF {table_name} FOR VALUES FROM ('{year}-{month}-01') to ('{year + int(month==12)}-{((month%12)+1)}-01')"
                        self.inform(sql_query)
                        with conn.cursor() as curs:
                            curs.execute(sql_query)
                            conn.commit()

            for create_index in indexes:
                with conn.cursor() as curs:
                    curs.execute(create_index)
                    conn.commit()
            conn.close()


    @retry(stop_max_attempt_number=10, wait_random_min=5000, wait_random_max=10000)
    def pgsql_upsert_records(self, df: pd.DataFrame = None, table: str = None, schema: str = None, dtypes: Dict[str, str] = {}, primary_keys: List[str] = [], varchar_length_source: Dict[str, int] = {}, logging_flag: bool = False, json_cols: List[str] = [], strict_mode: bool = False, partition_col: str = None, indexes: List[str] = []) -> None:
        if(df.empty):
            raise EmptyDataframe("Dataframe can not be empty.")
        try:
            df2 = df.copy()
            table = table.replace('.', '_').replace('-', '_')
            self.pgsql_create_table(df=df2, table=table, schema=schema, dtypes=dtypes, primary_keys=primary_keys, varchar_length_source = varchar_length_source, logging_flag=logging_flag, json_cols = json_cols, partition_col=partition_col, indexes = indexes)
            table_name = f"{schema}.{table}" if schema and len(schema) > 0 else table
            col_names = ""
            list_cols = df2.columns.to_list()
            for col in list_cols:
                col_names = col_names + col + ", "

            col_names = col_names[:-2]
            for key, val in dtypes.items():
                if(val == 'timestamp' or val == 'datetime'):
                    df2[key] = df2[key].apply(lambda x: x.astimezone(pytz.timezone(settings['timezone'])).strftime('%Y-%m-%d %H:%M:%S.%f') if not pd.isna(x) else '')

            cols_def = ""
            for _, row in df2.iterrows():
                row_def = "("
                for col in list_cols:
                    if(pd.isna(row[col]) or (not strict_mode and col in json_cols and len(row[col]) == 0)):
                        row_def += "NULL"
                    elif(col not in dtypes.keys() or dtypes[col] == 'string' or dtypes[col] == "str"):
                        row_def += "\'{0}\'".format(row[col].replace("'", "''"))
                    elif(dtypes[col] == 'timestamp' or dtypes[col] == "datetime"):
                        if(len(row[col]) > 0):
                            row_def += f"CAST(\'{row[col]}\' AS TIMESTAMP)"
                        else:
                            row_def += "NULL"
                    elif(dtypes[col] == 'boolean' or dtypes[col] == "bool"):
                        if(row[col]):
                            row_def += "True"
                        else:
                            row_def += "False"
                    elif(dtypes[col] == 'bigint' or dtypes[col] == "int"):
                        row_def += f'{int(row[col])}'
                    elif(dtypes[col] == 'double' or dtypes[col] == "float"):
                        row_def += f'{float(row[col])}'
                    row_def += ", "
                row_def = row_def[:-2] + "),\n"
                cols_def += row_def
            cols_def = cols_def[:-2]

            conflict_behaviour = "ON CONFLICT DO NOTHING"
            if(not logging_flag and len(primary_keys)>0):
                up_query = ""
                for col in list_cols:
                    if(col != primary_keys[0]):
                        up_query += f"{col} = excluded.{col},\n"
                if(len(up_query) > 2):
                    up_query = up_query[:-2]
                if(partition_col):
                    conflict_behaviour = f"ON CONFLICT ({primary_keys[0]}, {partition_col}) DO UPDATE SET {up_query}"
                else:
                    conflict_behaviour = f"ON CONFLICT ({primary_keys[0]}) DO UPDATE SET {up_query}"

            sql_query = f"INSERT INTO {table_name}({col_names})\nVALUES\n{cols_def}\n {conflict_behaviour};"
            conn = psycopg2.connect(
                host=self.db_destination['url'],
                database=self.db_destination['db_name'],
                user=self.db_destination['username'],
                password=self.db_destination['password']
            )
            with conn.cursor() as curs:
                curs.execute(sql_query)
                conn.commit()
            conn.close()
        except Exception as e:
            self.err(str(e))
            raise Exception("Unable to insert records in table.") from e


    def inform(self, message: str = "") -> None:
        logger.inform(s=f"{self.unique_id}: {message}")


    def warn(self, message: str = "") -> None:
        logger.warn(s=f"{self.unique_id}: {message}")


    def err(self, message: str = "") -> None:
        logger.err(s=f"{self.unique_id}: {message}")


    def save(self, processed_data: Dict[str, Any] = None, primary_keys: List[str] = None) -> None:
        if(not self.name_ or not(self.name_ == processed_data['name'])):
            self.table_list.extend(processed_data['name'])
        self.name_ = processed_data['name']

        logging_flag = False
        if('logging_flag' in processed_data.keys() and processed_data['logging_flag']):
            logging_flag = True

        varchar_length_source = processed_data['varchar_lengths'] if 'varchar_lengths' in processed_data and processed_data['varchar_lengths'] else {}
        if(not varchar_length_source):
            varchar_length_source = processed_data['lob_fields_length'] if 'lob_fields_length' in processed_data else {}

        json_cols = processed_data['json_cols'] if 'json_cols' in processed_data.keys() and processed_data['json_cols'] else []

        strict_mode = False
        if('strict' in processed_data.keys() and processed_data['strict']):
            strict_mode = True

        indexes = []
        if('indexes' in processed_data.keys() and processed_data['indexes']):
            indexes = processed_data['indexes']

        if('col_rename' in processed_data and processed_data['col_rename']):
            for key, val in processed_data['col_rename'].items():
                if(key in varchar_length_source.keys()):
                    varchar_length_source[val] = varchar_length_source[key]
                    varchar_length_source.pop(key)
                elif(key in json_cols):
                    ind = json_cols.index(key)
                    json_cols[ind] = val

        partition_col = None
        if('partition_col' in processed_data.keys() and processed_data['partition_col']):
            partition_col = processed_data['partition_col']

        if('df_insert' in processed_data and processed_data['df_insert'].shape[0] > 0):
            if('col_rename' in processed_data and processed_data['col_rename']):
                processed_data['df_insert'].rename(columns=processed_data['col_rename'], inplace=True)
            self.inform(message=f"Attempting to insert {str(processed_data['df_insert'].memory_usage(index=True).sum())} bytes.")
            self.pgsql_upsert_records(
                df=processed_data['df_insert'],
                table=self.name_,
                schema=self.schema,
                dtypes=processed_data['dtypes'],
                primary_keys=primary_keys,
                varchar_length_source=varchar_length_source,
                logging_flag=logging_flag,
                json_cols = json_cols,
                strict_mode = strict_mode,
                partition_col=partition_col,
                indexes = indexes
            )
            self.inform(message=f"Inserted {str(processed_data['df_insert'].shape[0])} records.")

        if('df_update' in processed_data and processed_data['df_update'].shape[0] > 0):
            self.inform(message=f"Attempting to update {str(processed_data['df_update'].memory_usage(index=True).sum())} bytes.")
            if('col_rename' in processed_data and processed_data['col_rename']):
                processed_data['df_update'].rename(columns=processed_data['col_rename'], inplace=True)
            # is_dump = False, and primary_keys will be present.
            self.pgsql_upsert_records(
                df=processed_data['df_update'],
                table=self.name_,
                schema=self.schema,
                dtypes=processed_data['dtypes'],
                primary_keys=primary_keys,
                varchar_length_source=varchar_length_source,
                logging_flag=logging_flag,
                json_cols = json_cols,
                strict_mode = strict_mode,
                partition_col=partition_col,
                indexes = indexes
            )
            self.inform(message=f"{str(processed_data['df_update'].shape[0])} updations done.")


    def delete_table(self, table_name: str = None) -> None:
        table_name = table_name.replace('.', '_').replace('-', '_')
        query = f"DROP TABLE  IF EXISTS {self.schema}.{table_name};"
        self.inform(query)
        conn = psycopg2.connect(
            host=self.db_destination['url'],
            database=self.db_destination['db_name'],
            user=self.db_destination['username'],
            password=self.db_destination['password']
        )
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
        conn.close()
        self.inform(f"Deleted {table_name} from PgSQL schema {self.schema}")

    def get_n_cols(self, table_name: str = None) -> int:
        table_name = table_name.replace('.', '_').replace('-', '_')
        query = f'SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = \'{self.schema}\' AND table_name = \'{table_name}\''
        self.inform(query)
        conn = psycopg2.connect(
            host=self.db_destination['url'],
            database=self.db_destination['db_name'],
            user=self.db_destination['username'],
            password=self.db_destination['password']
        )
        with conn.cursor() as cursor:
            cursor.execute(query)
            df = pd.DataFrame(cursor.fetchall(), columns=['count'])
        conn.close()
        return df.iloc[0][0]


    def is_exists(self, table_name: str = None) -> bool:
        try:
            table_name = table_name.replace('.', '_').replace('-', '_')
            sql_query = f'SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = \'{self.schema}\' AND TABLE_NAME = \'{table_name}\';'
            self.inform(sql_query)
            conn = psycopg2.connect(
                host=self.db_destination['url'],
                database=self.db_destination['db_name'],
                user=self.db_destination['username'],
                password=self.db_destination['password']
            )
            rows = []
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                rows = cursor.fetchall()
                if(not rows or len(rows) == 0):
                    return False
                else:
                    return True
        except Exception as e:
            self.err("Unable to check the presence of the table at destination.")
            raise


    def count_n_records(self, table_name: str = None) -> int:
        try:
            table_name = table_name.replace('.', '_').replace('-', '_')
            if(self.is_exists(table_name=table_name)):
                sql_query = f'SELECT COUNT(*) as count FROM {self.schema}.{table_name}'
                self.inform(sql_query)
                conn = psycopg2.connect(
                    host=self.db_destination['url'],
                    database=self.db_destination['db_name'],
                    user=self.db_destination['username'],
                    password=self.db_destination['password']
                )
                df = pd.DataFrame({})
                with conn.cursor() as cursor:
                    cursor.execute(sql_query)
                    df = pd.DataFrame(cursor.fetchall(), columns=['count'])
                return df.iloc[0][0]
            else:
                return 0
        except Exception as e:
            self.err("Unable to fetch the number of records previously at destination.")
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
        self.inform(message=f"Trying to expire data which was modified on or before {delete_before_date.strftime('%Y/%m/%d')}")
        # Expire function is called only when Mode = Dumping
        # i.e. the saved data will have a migration_snapshot_date column
        # We just have to query using that column to delete old data
        delete_before_date_str = delete_before_date.strftime('%Y-%m-%d %H:%M:%S')
        for table_name in self.table_list:
            query = f"DELETE FROM {self.schema}.{table_name} WHERE migration_snapshot_date <= {delete_before_date_str};"
            conn = psycopg2.connect(
                host=self.db_destination['url'],
                database=self.db_destination['db_name'],
                user=self.db_destination['username'],
                password=self.db_destination['password']
            )
            with conn.cursor() as cursor:
                cursor.execute(query)
            conn.close()

 
    def mirror_pkeys(self, table_name: str = None, primary_key: str = None, primary_key_dtype: str = None, data_df: pd.DataFrame = None):
        table_name = table_name.replace('.', '_').replace('-', '_')
        if(data_df.shape[0]):
            pkey_max = data_df[primary_key].max()
            if(primary_key_dtype == 'int'):
                str_pkey = str(pkey_max)
            elif(primary_key_dtype in ['str', 'uuid']):
                str_pkey = f"'{pkey_max}'"
            else:
                str_pkey = f"CAST('{str_pkey.strftime('%Y-%m-%d %H:%M:%S')}' as timestamp)"

            start = True
            del_pkeys_list = ""
            for key in data_df[primary_key].tolist():
                if(primary_key_dtype == 'int'):
                    key_str = str(key)
                elif(primary_key_dtype in ['str', 'uuid']):
                    key_str = f"'{key}'"
                else:
                    key_str = f"CAST('{key.strftime('%Y-%m-%d %H:%M:%S')}' as timestamp)"
                if(start):
                    del_pkeys_list = f"{key_str}"
                    start = False
                else:
                    del_pkeys_list = f"{del_pkeys_list}, {key_str}"

            sql_stmt = f"DELETE FROM {self.schema}.{table_name} WHERE {primary_key} <= {str_pkey} AND {primary_key} not in ({del_pkeys_list})"
            self.inform(sql_stmt)

            conn = psycopg2.connect(
                host=self.db_destination['url'],
                database=self.db_destination['db_name'],
                user=self.db_destination['username'],
                password=self.db_destination['password']
            )

            with conn.cursor() as cursor:
                cursor.execute(sql_stmt)
                conn.commit()

            self.inform(f"Deleted some records from destination which no longer exist at source.")
            conn.close()


    def close(self):
        pass
