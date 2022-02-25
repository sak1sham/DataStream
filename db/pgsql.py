import pandas as pd
import psycopg2
import pymongo

from helper.util import convert_list_to_string, convert_to_datetime, convert_to_dtype, utc_to_local
from db.encr_db import get_data_from_encr_db, get_last_run_cron_job
from helper.exceptions import *
from helper.logging import logger
from dst.main import DMS_exporter

import datetime
import hashlib
import pytz
from typing import List, Dict, Any, NewType, Tuple

dftype = NewType("dftype", pd.DataFrame)
collectionType =  NewType("collectionType", pymongo.collection.Collection)

class PGSQLMigrate:
    def __init__(self, db: Dict[str, Any] = None, table: Dict[str, Any] = None, batch_size: int = 1000, tz_str: str = 'Asia/Kolkata') -> None:
        self.db = db
        self.curr_mapping = table
        self.batch_size = batch_size
        if('batch_size' in table.keys()):
            self.batch_size = int(table['batch_size'])
        self.tz_info = pytz.timezone(tz_str)
        self.last_run_cron_job = pd.Timestamp(None)
    
    def inform(self, message: str = None) -> None:
        logger.inform(self.curr_mapping['unique_id'] + ": " + message)
    
    def warn(self, message: str = None) -> None:
        logger.warn(self.curr_mapping['unique_id'] + ": " + message)

    def preprocess(self) -> None:
        self.last_run_cron_job = get_last_run_cron_job(self.curr_mapping['unique_id'])
        self.curr_run_cron_job = utc_to_local(datetime.datetime.utcnow(), self.tz_info)
        self.saver = DMS_exporter(db = self.db, uid = self.curr_mapping['unique_id'])


    def distribute_records(self, collection_encr: collectionType = None, df: dftype = pd.DataFrame({})) -> Tuple[dftype]:
        df = df.sort_index(axis = 1)
        df_insert = pd.DataFrame({})
        df_update = pd.DataFrame({})
        for i in range(df.shape[0]):
            encr = {
                'table': self.curr_mapping['unique_id'],
                'map_id': df.iloc[i].unique_migration_record_id,
                'record_sha': hashlib.sha256(convert_list_to_string(df.loc[i, :].values.tolist()).encode()).hexdigest()
            }
            previous_records = collection_encr.find_one({'table': self.curr_mapping['unique_id'], 'map_id': df.iloc[i].unique_migration_record_id})
            if(previous_records):
                if(previous_records['record_sha'] == encr['record_sha']):
                    continue
                else:
                    df_update = df_update.append(df.loc[i, :])
                    collection_encr.delete_one({'table': self.curr_mapping['unique_id'], 'map_id': df.iloc[i].unique_migration_record_id})
                    collection_encr.insert_one(encr)
            else:
                df_insert = df_insert.append(df.loc[i, :])
                collection_encr.insert_one(encr)
        return df_insert, df_update

    def process_table(self, df: dftype = pd.DataFrame({}), table_name: str = None, col_dtypes: Dict[str, str] = {}) -> Dict[str, Any]:
        collection_encr = get_data_from_encr_db()
        last_run_cron_job = self.last_run_cron_job
        if('is_dump' in self.curr_mapping.keys() and self.curr_mapping['is_dump']):
            df['migration_snapshot_date'] = self.curr_run_cron_job
        self.partition_for_parquet = []
        if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):
            if('partition_col' in self.curr_mapping.keys()):
                if(isinstance(self.curr_mapping['partition_col'], str)):
                    self.curr_mapping['partition_col'] = [self.curr_mapping['partition_col']]
                if('partition_col_format' not in self.curr_mapping.keys()):
                    self.curr_mapping['partition_col_format'] = ['str']
                if(isinstance(self.curr_mapping['partition_col_format'], str)):
                    self.curr_mapping['partition_col_format'] = [self.curr_mapping['partition_col_format']]
                while(len(self.curr_mapping['partition_col']) > len(self.curr_mapping['partition_col_format'])):
                    self.curr_mapping['partition_col_format'] = self.curr_mapping['partition_col_format'].append('str')
                for i in range(len(self.curr_mapping['partition_col'])):
                    col = self.curr_mapping['partition_col'][i].lower()
                    col_form = self.curr_mapping['partition_col_format'][i]
                    parq_col = "parquet_format_" + col
                    if(col == 'migration_snapshot_date'):
                        self.partition_for_parquet.extend([parq_col + "_year", parq_col + "_month", parq_col + "_day", parq_col + "_hour"])
                        temp = df[col].apply(lambda x: convert_to_datetime(x, self.tz_info))
                        df[parq_col + "_year"] = temp.dt.year
                        df[parq_col + "_month"] = temp.dt.month
                        df[parq_col + "_day"] = temp.dt.day
                        df[parq_col + "_hour"] = temp.dt.hour
                    elif(col_form == 'str'):
                        self.partition_for_parquet.extend([parq_col])
                        df[parq_col] = df[col].astype(str)
                    elif(col_form == 'int'):
                        self.partition_for_parquet.extend([parq_col])
                        df[parq_col] = df[col].astype(int)
                    elif(col_form == 'datetime'):
                        self.partition_for_parquet.extend([parq_col + "_year", parq_col + "_month", parq_col + "_day", parq_col + "_hour"])
                        temp = df[col].apply(lambda x: convert_to_datetime(x, self.tz_info))
                        df[parq_col + "_year"] = temp.dt.year
                        df[parq_col + "_month"] = temp.dt.month
                        df[parq_col + "_day"] = temp.dt.day
                        df[parq_col + "_hour"] = temp.dt.hour
                    else:
                        raise UnrecognizedFormat(str(col_form) + ". Partition_col_format can be int, float, str or datetime.") 
            else:
                self.warn("Unable to find partition_col. Continuing without partitioning.")
        df_consider = df
        df_insert = pd.DataFrame({})
        df_update = pd.DataFrame({})
        if('is_dump' not in self.curr_mapping.keys() or not self.curr_mapping['is_dump']):
            if('primary_keys' not in self.curr_mapping):
                self.curr_mapping['primary_keys'] = df.columns.values.tolist()
                self.warn("Unable to find primary_keys in mapping. Taking entire records into consideration.")
            if(isinstance(self.curr_mapping['primary_keys'], str)):
                self.curr_mapping['primary_keys'] = [self.curr_mapping['primary_keys']]
            self.curr_mapping['primary_keys'] = [x.lower() for x in self.curr_mapping['primary_keys']]
            df['unique_migration_record_id'] = df[self.curr_mapping['primary_keys']].astype(str).sum(1)
            if('bookmark' in self.curr_mapping.keys() and self.curr_mapping['bookmark']):
                df_consider = df[df[self.curr_mapping['bookmark']].apply(lambda x: convert_to_datetime(x, self.tz_info)) > last_run_cron_job]
                if('bookmark_creation' in self.curr_mapping.keys() and self.curr_mapping['bookmark_creation']):
                    df_insert = df_consider[df_consider[self.curr_mapping['bookmark_creation']].apply(lambda x: convert_to_datetime(x, self.tz_info)) > last_run_cron_job]
                    df_update = df_consider[df_consider[self.curr_mapping['bookmark_creation']].apply(lambda x: convert_to_datetime(x, self.tz_info)) <= last_run_cron_job]
                else:
                    df_insert, df_update = self.distribute_records(collection_encr, df_consider)
            else:
                if('bookmark_creation' in self.curr_mapping.keys() and self.curr_mapping['bookmark_creation']):
                    df_insert = df[df[self.curr_mapping['bookmark_creation']].apply(lambda x: convert_to_datetime(x, self.tz_info)) > last_run_cron_job]
                    df_consider = df[df[self.curr_mapping['bookmark_creation']].apply(lambda x: convert_to_datetime(x, self.tz_info)) <= last_run_cron_job]
                    _, df_update = self.distribute_records(collection_encr, df_consider)
                else:
                    df_insert, df_update = self.distribute_records(collection_encr, df_consider)
        else:
            df_insert = df_consider
            df_update = pd.DataFrame({})
        df_insert = convert_to_dtype(df_insert, col_dtypes)
        df_update = convert_to_dtype(df_update, col_dtypes)
        return {'name': table_name, 'df_insert': df_insert, 'df_update': df_update}


    def save_data(self, processed_data: Dict[str, Any] = None, c_partition: List[str] = None) -> None:
        if(not processed_data):
            return
        else:
            self.saver.save(processed_data = processed_data, primary_keys = ['unique_migration_record_id'], c_partition = c_partition)

    
    def get_list_tables(self) -> List[str]:
        sql_stmt = '''
            SELECT schemaname, tablename
            FROM pg_catalog.pg_tables
            WHERE schemaname != 'pg_catalog' AND 
            schemaname != 'information_schema';
        '''
        if('username' not in self.db['source'].keys()):
            self.db['source']['username'] = ''
        if('password' not in self.db['source'].keys()):
            self.db['source']['password'] = ''
        try:
            conn = psycopg2.connect(
                host = self.db['source']['url'],
                database = self.db['source']['db_name'],
                user = self.db['source']['username'],
                password = self.db['source']['password']
            )
            try:            
                with conn.cursor() as curs:
                    curs.execute(sql_stmt)
                    rows = curs.fetchall()
                    table_names = [str(t[0] + "." + t[1]) for t in rows]
                    return table_names
            except Exception as e:
                raise ProcessingError("Caught some exception while getting list of all tables.")
        except Exception as e:
            raise ConnectionError("Unable to connect to source.")

    def get_column_dtypes(self, conn: Any = None, curr_table_name: str = None) -> Dict[str, str]:
        if('fetch_data_query' in self.curr_mapping.keys() and isinstance(self.curr_mapping['fetch_data_query'], str) and len(self.curr_mapping['fetch_data_query']) > 0):
            return {}
        tn = curr_table_name.split('.')
        schema_name = 'public'
        table_name = ''
        if(len(tn) > 1):
            schema_name = tn[0]
            table_name = tn[1]
        else:
            # if table_name doesn't contain schema name
            table_name = tn[0]
        sql_stmt = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = \'" + table_name + "\' AND table_schema = \'" + schema_name + "\';"
        col_dtypes = {}
        with conn.cursor('cursor-to-get-col-names') as curs:
            curs.execute(sql_stmt)
            rows = curs.fetchall()
            for key, val in rows:
                col_dtypes[key] = val
        return col_dtypes

    def migrate_data(self, table_name: str = None) -> None:
        self.inform("Migrating table " + table_name + ".")
        sql_stmt = "SELECT * FROM " + table_name
        if('fetch_data_query' in self.curr_mapping.keys() and self.curr_mapping['fetch_data_query'] and len(self.curr_mapping['fetch_data_query']) > 0):
            sql_stmt = self.curr_mapping['fetch_data_query']

        if('username' not in self.db['source'].keys()):
            self.db['source']['username'] = ''
        if('password' not in self.db['source'].keys()):
            self.db['source']['password'] = ''

        try:
            conn = psycopg2.connect(
                host = self.db['source']['url'],
                database = self.db['source']['db_name'],
                user = self.db['source']['username'],
                password = self.db['source']['password']
            )
            try:
                col_dtypes = self.get_column_dtypes(conn = conn, curr_table_name = table_name)
                with conn.cursor('cursor-name', scrollable = True) as curs:
                    curs.itersize = 2
                    curs.execute(sql_stmt)
                    _ = curs.fetchone()
                    columns = [desc[0] for desc in curs.description]
                    curs.scroll(-1)
                    while(True):
                        rows = curs.fetchmany(self.batch_size)
                        if (not rows):
                            break
                        else:
                            data_df = pd.DataFrame(rows, columns = columns)
                            processed_data = self.process_table(df = data_df, table_name = table_name, col_dtypes = col_dtypes)
                            self.save_data(processed_data = processed_data, c_partition = self.partition_for_parquet)
                self.inform("Completed migration of table " + table_name + ".\n")
            except Exception as e:
                raise ProcessingError("Caught some exception while processing records.")
        except Exception as e:
            raise ConnectionError("Unable to connect to source.")


    def process(self) -> None:
        name_tables = []
        if(self.curr_mapping['table_name'] == '*'):
            name_tables = self.get_list_tables()
        else:
            name_tables = [self.curr_mapping['table_name']]
        name_tables.sort()
        self.inform("Found following " + str(len(name_tables)) + " tables from database " + str(self.db['source']['db_name']) + ":\n" + '\n'.join(name_tables))
        
        b_start = 0
        b_end = len(name_tables)
        if('batch_start' in self.curr_mapping.keys()):
            b_start = self.curr_mapping['batch_start']
        if('batch_end' in self.curr_mapping.keys()):
            b_end = self.curr_mapping['batch_end']
        name_tables = name_tables[b_start:b_end]
        self.preprocess()
        self.inform("Mapping pre-processed.")
        
        if('exclude_tables' not in self.curr_mapping.keys()):
            self.curr_mapping['exclude_tables'] = []
        elif(isinstance(self.curr_mapping['exclude_tables'], str)):
            self.curr_mapping['exclude_tables'] = [self.curr_mapping['exclude_tables']]
        useful_tables = []
        for name_ in name_tables:
            if(name_ not in self.curr_mapping['exclude_tables']):
                useful_tables.append(name_)
        name_tables = useful_tables
        name_tables.sort()
        self.inform("Starting to migrating following " + str(len(name_tables)) + " useful tables from database " + str(self.db['source']['db_name']) + ":\n" + '\n'.join(name_tables))
        
        for table_name in name_tables:
            self.migrate_data(table_name)

        self.inform("Overall migration complete.")
        if('is_dump' in self.curr_mapping.keys() and self.curr_mapping['is_dump'] and 'expiry' in self.curr_mapping.keys() and self.curr_mapping['expiry']):
            self.saver.expire(expiry = self.curr_mapping['expiry'], tz_info = self.tz_info)
            self.inform("Expired data removed.")
        self.saver.close()
        self.inform("Hope to see you again :')")
