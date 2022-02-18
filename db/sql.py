from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import pandas.io.sql as sqlio
import pymongo

import logging
logging.getLogger().setLevel(logging.INFO)
import traceback

from dst.s3 import s3Saver
from dst.redshift import RedshiftSaver
from helper.util import convert_list_to_string, convert_to_datetime
from db.encr_db import get_data_from_encr_db, get_last_run_cron_job

import datetime
import hashlib
import pytz
from typing import List, Dict, Any, NewType, Tuple


dftype = NewType("dftype", pd.DataFrame)
collectionType =  NewType("collectionType", pymongo.collection.Collection)

class ConnectionError(Exception):
    pass

class UnrecognizedFormat(Exception):
    pass

class DestinationNotFound(Exception):
    pass


class SQLMigrate:
    def __init__(self, db: Dict[str, Any], table: Dict[str, Any], batch_size: int = 10000, tz_str: str = 'Asia/Kolkata') -> None:
        self.db = db
        self.table = table
        self.batch_size = batch_size
        self.tz_info = pytz.timezone(tz_str)
    
    def inform(self, message: str) -> None:
        logging.info(self.table['table_unique_id'] + ": " + message)
    
    def warn(self, message: str) -> None:
        logging.warning(self.table['table_unique_id'] + ": " + message)


    def get_number_of_records(self) -> None:
        if('fetch_data_query' in self.table.keys() and self.table['fetch_data_query'] and len(self.table['fetch_data_query']) > 0):
            self.total_records = 1
        else:
            if('username' not in self.db['source'].keys() or 'password' not in self.db['source'].keys()):
                try:
                    engine = create_engine(self.db['source']['url'])
                    self.total_records = engine.execute("SELECT COUNT(*) from " + self.table['table_name']).fetchall()[0][0]
                except Exception as e:
                    self.total_records = 0
                    raise ConnectionError("Unable to connect to source.")
            else:
                try:
                    conn = psycopg2.connect(
                        host = self.db['source']['url'],
                        database = self.db['source']['db_name'],
                        user = self.db['source']['username'],
                        password = self.db['source']['password'])
                    cursor = conn.cursor()
                    total_records = cursor.execute("SELECT COUNT(*) from " + self.table['table_name'] + ";", [])
                    self.total_records = cursor.fetchone()[0]
                except Exception as e:
                    self.total_records = 0
                    raise ConnectionError("Unable to connect to source.")


    def get_data(self, start: int = 0) -> dftype:
        if('username' not in self.db['source'].keys() or 'password' not in self.db['source'].keys()):
            try:
                engine = create_engine(self.db['source']['url'])
                sql_stmt = "SELECT * FROM " + self.table['table_name'] + " LIMIT " + str(self.batch_size) + " OFFSET " + str(start)
                if('fetch_data_query' in self.table.keys() and self.table['fetch_data_query'] and len(self.table['fetch_data_query']) > 0):
                    sql_stmt = self.table['fetch_data_query']
                df = pd.read_sql(sql_stmt, engine)
                return df
            except Exception as e:
                raise ConnectionError("Unable to connect to source.")
        else:
            try:
                conn = psycopg2.connect(
                    host = self.db['source']['url'],
                    database = self.db['source']['db_name'],
                    user = self.db['source']['username'],
                    password = self.db['source']['password'])            
                select_query = "SELECT * FROM " + self.table['table_name'] + " LIMIT " + str(self.batch_size) + " OFFSET " + str(start)
                if('fetch_data_query' in self.table.keys() and self.table['fetch_data_query'] and len(self.table['fetch_data_query']) > 0):
                    select_query = self.table['fetch_data_query']
                df = sqlio.read_sql_query(select_query, conn)
                return df
            except Exception as e:
                raise ConnectionError("Unable to connect to source.")


    def preprocess(self) -> None:
        self.last_run_cron_job = get_last_run_cron_job(self.table['table_unique_id'])
        if('fetch_data_query' not in self.table.keys() or not self.table['fetch_data_query']):
            self.inform("Number of records: " + str(self.total_records))
        if(self.db['destination']['destination_type'] == 's3'):
            self.saver = s3Saver(db_source = self.db['source'], db_destination = self.db['destination'], unique_id = self.table['table_unique_id'])
        elif(self.db['destination']['destination_type'] == 'redshift'):
            self.saver = RedshiftSaver(db_source = self.db['source'], db_destination = self.db['destination'], unique_id = self.table['table_unique_id'])
        else:
            raise DestinationNotFound("Destination type not recognized. Choose from s3, redshift")
        self.inform("Table pre-processed.")


    def distribute_records(self, collection_encr: collectionType = None, df: dftype = pd.DataFrame({})) -> Tuple[dftype]:
        df = df.sort_index(axis = 1)
        df_insert = pd.DataFrame({})
        df_update = pd.DataFrame({})
        for i in range(df.shape[0]):
            encr = {
                'table': self.table['table_unique_id'],
                'map_id': df.iloc[i].unique_migration_record_id,
                'record_sha': hashlib.sha256(convert_list_to_string(df.loc[i, :].values.tolist()).encode()).hexdigest()
            }
            previous_records = collection_encr.find_one({'table': self.table['table_unique_id'], 'map_id': df.iloc[i].unique_migration_record_id})
            if(previous_records):
                if(previous_records['record_sha'] == encr['record_sha']):
                    continue
                else:
                    df_update = df_update.append(df.loc[i, :])
                    collection_encr.delete_one({'table': self.table['table_unique_id'], 'map_id': df.iloc[i].unique_migration_record_id})
                    collection_encr.insert_one(encr)
            else:
                df_insert = df_insert.append(df.loc[i, :])
                collection_encr.insert_one(encr)
        return df_insert, df_update


    def process_table(self, df: dftype = pd.DataFrame({})) -> Tuple[dftype]:
        if('fetch_data_query' in self.table.keys() and self.table['fetch_data_query']):
            self.inform("Number of records: " + str(df.shape[0]))
        collection_encr = get_data_from_encr_db()
        last_run_cron_job = self.last_run_cron_job
        if('is_dump' in self.table.keys() and self.table['is_dump']):
            df['migration_snapshot_date'] = datetime.datetime.utcnow().replace(tzinfo = self.tz_info)
        self.partition_for_parquet = []
        if('to_partition' in self.table.keys() and self.table['to_partition']):
            if('partition_col' in self.table.keys()):
                if(isinstance(self.table['partition_col'], str)):
                    self.table['partition_col'] = [self.table['partition_col']]
                if('partition_col_format' not in self.table.keys()):
                    self.table['partition_col_format'] = ['str']
                if(isinstance(self.table['partition_col_format'], str)):
                    self.table['partition_col_format'] = [self.table['partition_col_format']]
                while(len(self.table['partition_col']) > len(self.table['partition_col_format'])):
                    self.table['partition_col_format'] = self.table['partition_col_format'].append('str')
                for i in range(len(self.table['partition_col'])):
                    col = self.table['partition_col'][i].lower()
                    col_form = self.table['partition_col_format'][i]
                    parq_col = "parquet_format_" + col
                    if(col == 'migration_snapshot_date'):
                        self.partition_for_parquet.extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                        temp = df[col].apply(lambda x: convert_to_datetime(x, self.tz_info))
                        df[parq_col + "_year"] = temp.dt.year
                        df[parq_col + "_month"] = temp.dt.month
                        df[parq_col + "_day"] = temp.dt.day
                    elif(col_form == 'str'):
                        self.partition_for_parquet.extend([parq_col])
                        df[parq_col] = df[col].astype(str)
                    elif(col_form == 'int'):
                        self.partition_for_parquet.extend([parq_col])
                        df[parq_col] = df[col].astype(int)
                    elif(col_form == 'datetime'):
                        self.partition_for_parquet.extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                        temp = df[col].apply(lambda x: convert_to_datetime(x, self.tz_info))
                        df[parq_col + "_year"] = temp.dt.year
                        df[parq_col + "_month"] = temp.dt.month
                        df[parq_col + "_day"] = temp.dt.day
                    else:
                        raise UnrecognizedFormat(str(col_form) + ". Partition_col_format can be int, float, str or datetime.") 
            else:
                self.warn("Unable to find partition_col. Continuing without partitioning.")
        df_consider = df
        df_insert = pd.DataFrame({})
        df_update = pd.DataFrame({})
        if('is_dump' not in self.table.keys() or not self.table['is_dump']):
            if('primary_keys' not in self.table):
                self.table['primary_keys'] = df.columns.values.tolist()
                self.warn("Unable to find primary_keys in mapping. Taking entire records into consideration.")
            if(isinstance(self.table['primary_keys'], str)):
                self.table['primary_keys'] = [self.table['primary_keys']]
            self.table['primary_keys'] = [x.lower() for x in self.table['primary_keys']]
            df['unique_migration_record_id'] = df[self.table['primary_keys']].astype(str).sum(1)
            if('bookmark' in self.table.keys() and self.table['bookmark']):
                df_consider = df[df[self.table['bookmark']].apply(lambda x: convert_to_datetime(x, self.tz_info)) > last_run_cron_job]
                if('bookmark_creation' in self.table.keys() and self.table['bookmark_creation']):
                    df_insert = df_consider[df_consider[self.table['bookmark_creation']].apply(lambda x: convert_to_datetime(x, self.tz_info)) > last_run_cron_job]
                    df_update = df_consider[df_consider[self.table['bookmark_creation']].apply(lambda x: convert_to_datetime(x, self.tz_info)) <= last_run_cron_job]
                else:
                    df_insert, df_update = self.distribute_records(collection_encr, df_consider)
            else:
                if('bookmark_creation' in self.table.keys() and self.table['bookmark_creation']):
                    df_insert = df[df[self.table['bookmark_creation']].apply(lambda x: convert_to_datetime(x, self.tz_info)) > last_run_cron_job]
                    df_consider = df[df[self.table['bookmark_creation']].apply(lambda x: convert_to_datetime(x, self.tz_info)) <= last_run_cron_job]
                    _, df_update = self.distribute_records(collection_encr, df_consider)
                else:
                    df_insert, df_update = self.distribute_records(collection_encr, df_consider)
        else:
            df_insert = df_consider
            df_update = pd.DataFrame({})
        return {'name': self.table['table_name'], 'df_insert': df_insert, 'df_update': df_update}


    def save_data(self, processed_data: Dict[str, Any] = {}, c_partition: List[str] = []) -> None:
        if(not processed_data):
            return
        else:
            self.saver.save(processed_data, c_partition)
            self.saver.close()


    def process(self) -> None:
        self.get_number_of_records()
        self.preprocess()
        if(self.total_records and self.total_records > 0):
            start = 0
            while(start < self.total_records):
                df = self.get_data(start=start)
                if(df is not None):
                    processed_data = self.process_table(df = df)
                    self.save_data(processed_data = processed_data, c_partition = self.partition_for_parquet)
                start += self.batch_size
        self.inform("Migration Complete.")
        if('is_dump' in self.table.keys() and self.table['is_dump'] and 'expiry' in self.table.keys()):
            self.saver.expire(self.table['expiry'], self.tz_info)
            self.inform("Expired data removed.")
        self.inform("\n\n")


def process_sql_table(db: Dict[str, Any] = {}, table: Dict[str, Any] = {}) -> None:
    obj = SQLMigrate(db, table)
    try:
        obj.process()
    except Exception as e:
        logging.error(traceback.format_exc())
        logging.info(table['table_unique_id'] + ": Migration stopped.\n\n")