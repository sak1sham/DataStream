import pandas as pd
import psycopg2
import pymongo
import traceback

from dst.s3 import s3Saver
from dst.redshift import RedshiftSaver
from helper.util import convert_list_to_string, convert_to_datetime
from db.encr_db import get_data_from_encr_db, get_last_run_cron_job
from helper.exceptions import *
from helper.logging import logger

import datetime
import hashlib
import pytz
from typing import List, Dict, Any, NewType, Tuple

dftype = NewType("dftype", pd.DataFrame)
collectionType =  NewType("collectionType", pymongo.collection.Collection)

class SQLMigrate:
    def __init__(self, db: Dict[str, Any], table: Dict[str, Any], batch_size: int = 10000, tz_str: str = 'Asia/Kolkata') -> None:
        self.db = db
        self.table = table
        self.batch_size = batch_size
        self.tz_info = pytz.timezone(tz_str)
    
    def inform(self, message: str) -> None:
        logger.inform(self.table['unique_id'] + ": " + message)
    
    def warn(self, message: str) -> None:
        logger.warn(self.table['unique_id'] + ": " + message)

    def preprocess(self) -> None:
        self.last_run_cron_job = get_last_run_cron_job(self.table['unique_id'])
        if(self.db['destination']['destination_type'] == 's3'):
            self.saver = s3Saver(db_source = self.db['source'], db_destination = self.db['destination'], unique_id = self.table['unique_id'])
        elif(self.db['destination']['destination_type'] == 'redshift'):
            self.saver = RedshiftSaver(db_source = self.db['source'], db_destination = self.db['destination'], unique_id = self.table['unique_id'])
        else:
            raise DestinationNotFound("Destination type not recognized. Choose from s3, redshift.")


    def distribute_records(self, collection_encr: collectionType = None, df: dftype = pd.DataFrame({})) -> Tuple[dftype]:
        df = df.sort_index(axis = 1)
        df_insert = pd.DataFrame({})
        df_update = pd.DataFrame({})
        for i in range(df.shape[0]):
            encr = {
                'table': self.table['unique_id'],
                'map_id': df.iloc[i].unique_migration_record_id,
                'record_sha': hashlib.sha256(convert_list_to_string(df.loc[i, :].values.tolist()).encode()).hexdigest()
            }
            previous_records = collection_encr.find_one({'table': self.table['unique_id'], 'map_id': df.iloc[i].unique_migration_record_id})
            if(previous_records):
                if(previous_records['record_sha'] == encr['record_sha']):
                    continue
                else:
                    df_update = df_update.append(df.loc[i, :])
                    collection_encr.delete_one({'table': self.table['unique_id'], 'map_id': df.iloc[i].unique_migration_record_id})
                    collection_encr.insert_one(encr)
            else:
                df_insert = df_insert.append(df.loc[i, :])
                collection_encr.insert_one(encr)
        return df_insert, df_update


    def process_table(self, df: dftype = pd.DataFrame({})) -> Dict[str, Any]:
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
            if(self.db['destination']['destination_type'] == 's3'):
                self.saver.save(processed_data, c_partition)
            elif(self.db['destination']['destination_type'] == 'redshift'):
                if('is_dump' in self.table.keys() and self.table['is_dump']):
                    self.saver.save(processed_data)
                else:
                    ## In case of syncing (not simply dumping) with redshift, we need to specify some primary keys for it to do the updations
                    self.saver.save(processed_data, ['unique_migration_record_id'])


    def migrate_data(self) -> None:
        sql_stmt = "SELECT * FROM " + self.table['table_name']
        if('fetch_data_query' in self.table.keys() and self.table['fetch_data_query'] and len(self.table['fetch_data_query']) > 0):
            sql_stmt = self.table['fetch_data_query']

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
                    columns = [desc[0] for desc in curs.description]
                    while(True):
                        rows = curs.fetchmany(self.batch_size)
                        if (not rows):
                            break
                        else:
                            data_df = pd.DataFrame(rows, columns = columns)
                            processed_data = self.process_table(df = data_df)
                            self.save_data(processed_data = processed_data, c_partition = self.partition_for_parquet)
            except Exception as e:
                raise ProcessingError("Caught some exception while processing records.")
        except Exception as e:
            raise ConnectionError("Unable to connect to source.")


    def process(self) -> None:
        self.preprocess()
        self.inform("Table pre-processed.")
        self.migrate_data()
        self.inform("Migration Complete.")
        if('is_dump' in self.table.keys() and self.table['is_dump'] and 'expiry' in self.table.keys() and self.table['expiry']):
            self.saver.expire(self.table['expiry'], self.tz_info)
            self.inform("Expired data removed.")
        self.saver.close()
        self.inform("Hope to see you again :')\n")


def process_sql_table(db: Dict[str, Any] = {}, table: Dict[str, Any] = {}) -> None:
    obj = SQLMigrate(db, table)
    try:
        obj.process()
    except Exception as e:
        logger.err(traceback.format_exc())
        logger.inform(table['unique_id'] + ": Migration stopped.\n")

