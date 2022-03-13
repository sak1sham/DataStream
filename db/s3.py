import pandas as pd
import pymongo
import awswrangler as wr

from helper.util import convert_list_to_string, convert_to_datetime, convert_to_dtype, convert_to_utc, utc_to_local, get_athena_dtypes
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

class S3Migrate:
    def __init__(self, db: Dict[str, Any] = None, curr_mapping: Dict[str, Any] = None, batch_size: int = 1000, tz_str: str = 'Asia/Kolkata') -> None:
        self.db = db
        self.curr_mapping = curr_mapping
        if('batch_size' in curr_mapping.keys()):
            self.batch_size = int(curr_mapping['batch_size'])
        else:
            self.batch_size = batch_size
        self.tz_info = pytz.timezone(tz_str)
        self.last_run_cron_job = pd.Timestamp(None)
    
    def inform(self, message: str = None) -> None:
        logger.inform(self.curr_mapping['unique_id'] + ": " + message)
    
    def warn(self, message: str = None) -> None:
        logger.warn(self.curr_mapping['unique_id'] + ": " + message)

    def err(self, error: Any = None) -> None:
        logger.err(error)

    def preprocess(self) -> None:
        self.last_run_cron_job = convert_to_datetime(get_last_run_cron_job(self.curr_mapping['unique_id']), self.tz_info)
        self.curr_run_cron_job = convert_to_datetime(utc_to_local(datetime.datetime.utcnow(), self.tz_info), self.tz_info)
        self.last_modified_begin = convert_to_utc(self.last_run_cron_job)
        self.last_modified_end = convert_to_utc(self.curr_run_cron_job)
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
        if('is_dump' not in self.curr_mapping.keys() or not self.curr_mapping['is_dump']):
            if('primary_keys' not in self.curr_mapping):
                self.curr_mapping['primary_keys'] = df.columns.values.tolist()
                self.warn("Unable to find primary_keys in mapping. Taking entire records into consideration.")
            if(isinstance(self.curr_mapping['primary_keys'], str)):
                self.curr_mapping['primary_keys'] = [self.curr_mapping['primary_keys']]
            self.curr_mapping['primary_keys'] = [x.lower() for x in self.curr_mapping['primary_keys']]
            df['unique_migration_record_id'] = df[self.curr_mapping['primary_keys']].astype(str).sum(1)
            if('bookmark' in self.curr_mapping.keys() and self.curr_mapping['bookmark']):
                df_consider = df[df[self.curr_mapping['bookmark']].apply(lambda x: convert_to_datetime(x, self.tz_info)) > self.last_run_cron_job]
                df_consider = df[df[self.curr_mapping['bookmark']].apply(lambda x: convert_to_datetime(x, self.tz_info)) <= self.curr_run_cron_job]
                if('bookmark_creation' in self.curr_mapping.keys() and self.curr_mapping['bookmark_creation']):
                    df_insert = df_consider[df_consider[self.curr_mapping['bookmark_creation']].apply(lambda x: convert_to_datetime(x, self.tz_info)) > self.last_run_cron_job]
                    df_update = df_consider[df_consider[self.curr_mapping['bookmark_creation']].apply(lambda x: convert_to_datetime(x, self.tz_info)) <= self.last_run_cron_job]
                else:
                    df_insert, df_update = self.distribute_records(collection_encr, df_consider)
            else:
                if('bookmark_creation' in self.curr_mapping.keys() and self.curr_mapping['bookmark_creation']):
                    df_consider = df[df[self.curr_mapping['bookmark_creation']].apply(lambda x: convert_to_datetime(x, self.tz_info)) <= self.curr_run_cron_job]
                    df_insert = df[df[self.curr_mapping['bookmark_creation']].apply(lambda x: convert_to_datetime(x, self.tz_info)) > self.last_run_cron_job]
                    df_consider = df[df[self.curr_mapping['bookmark_creation']].apply(lambda x: convert_to_datetime(x, self.tz_info)) <= self.last_run_cron_job]
                    _, df_update = self.distribute_records(collection_encr, df_consider)
                else:
                    df_insert, df_update = self.distribute_records(collection_encr, df_consider)
        else:
            df_insert = df_consider
            df_update = pd.DataFrame({})
        df_insert = convert_to_dtype(df_insert, col_dtypes)
        df_update = convert_to_dtype(df_update, col_dtypes)
        dtypes = get_athena_dtypes(col_dtypes)
        return {'name': table_name, 'df_insert': df_insert, 'df_update': df_update, 'dtypes': dtypes}

    def save_data(self, processed_data: Dict[str, Any] = None, c_partition: List[str] = None) -> None:
        if(not processed_data):
            return
        else:
            primary_keys = []
            if('is_dump' not in self.curr_mapping.keys() or not self.curr_mapping['is_dump']):
                primary_keys = ['unique_migration_record_id']
            self.saver.save(processed_data = processed_data, primary_keys = primary_keys, c_partition = c_partition)
    
    def migrate_data(self) -> None:
        self.inform("Migrating table " + self.curr_mapping['table_name'] + ".")
        dfs = [pd.DataFrame({})]
        try:
            if('is_dump' in self.curr_mapping.keys() and self.curr_mapping['is_dump']):
                dfs = wr.s3.read_parquet(path=self.db['source']['url'], path_suffix='.parquet', ignore_empty=True, chunked=self.batch_size, dataset=True, last_modified_end=self.last_modified_end)
            else:
                dfs = wr.s3.read_parquet(path=self.db['source']['url'], path_suffix='.parquet', ignore_empty=True, chunked=self.batch_size, dataset=True, last_modified_begin=self.last_modified_begin, last_modified_end=self.last_modified_end)
        except wr.exceptions.NoFilesFound:
            self.inform("No new/relevant files found at source which DMS can migrate.")
        except Exception as e:
            self.err(e)
            raise ConnectionError("Unable to connect to source.")
        else:
            try:
                for df in dfs:
                    processed_data = self.process_table(df = df, table_name = self.curr_mapping['table_name'], col_dtypes = self.curr_mapping['fields'])
                    self.save_data(processed_data = processed_data, c_partition = self.partition_for_parquet)
            except Exception as e:
                self.err(e)
                raise ProcessingError("Caught some exception while processing records.")
        
    def process(self) -> None:
        self.preprocess()
        self.inform("Mapping pre-processed.")
        self.migrate_data()
        self.inform("Migration complete.")
        if('is_dump' in self.curr_mapping.keys() and self.curr_mapping['is_dump'] and 'expiry' in self.curr_mapping.keys() and self.curr_mapping['expiry']):
            self.saver.expire(expiry = self.curr_mapping['expiry'], tz_info = self.tz_info)
            self.inform("Expired data removed.")
        self.saver.close()
        self.inform("Hope to see you again :')")