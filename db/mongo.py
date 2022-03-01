from helper.util import validate_or_convert, convert_to_datetime, utc_to_local, typecast_df_to_schema
from db.encr_db import get_data_from_encr_db, get_last_run_cron_job
from helper.exceptions import *
from helper.logging import logger
from dst.main import DMS_exporter

from pymongo import MongoClient
import certifi
import pandas as pd
import datetime
import json
import hashlib
import pytz
from typing import Dict, Any
import time

class MongoMigrate:
    def __init__(self, db: Dict[str, Any] = None, curr_mapping: Dict[str, Any] = None, batch_size: int = 10000, tz_str: str = 'Asia/Kolkata') -> None:
        if (not db or not curr_mapping):
            raise MissingData("db or curr_mapping can not be None.")
        if('batch_size' in curr_mapping.keys() and isinstance(curr_mapping['batch_size'], int)):
            self.batch_size = curr_mapping['batch_size']
        else:
            self.batch_size = batch_size
        if('time_delay' in curr_mapping.keys() and isinstance(curr_mapping['time_delay'], int)):
            self.time_delay = curr_mapping['time_delay']
        else:
            self.time_delay = 0
        self.db = db
        self.curr_mapping = curr_mapping
        self.tz_info = pytz.timezone(tz_str)

    def inform(self, message: str = None) -> None:
        logger.inform(self.curr_mapping['unique_id'] + ": " + message)

    def warn(self, message: str = None) -> None:
        logger.warn(self.curr_mapping['unique_id'] + ": " + message)

    def err(self, error: Any = None) -> None:
        logger.err(error)

    def get_data(self) -> None:
        try:
            client = MongoClient(self.db['source']['url'], tlsCAFile=certifi.where())
            database_ = client[self.db['source']['db_name']]
            self.db_collection = database_[self.curr_mapping['collection_name']]
        except Exception as e:
            self.err(e)
            self.db_collection = None
            raise ConnectionError("Unable to connect to source.")

    def preprocess(self) -> None:
        if('fields' not in self.curr_mapping.keys()):
            self.curr_mapping['fields'] = {}
        
        self.last_run_cron_job = utc_to_local(get_last_run_cron_job(self.curr_mapping['unique_id']), self.tz_info)
        self.curr_run_cron_job = utc_to_local(datetime.datetime.utcnow(), self.tz_info)
        self.partition_for_parquet = []

        if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):
            if('partition_col' not in self.curr_mapping.keys() or not self.curr_mapping['partition_col']):
                self.warn("Partition_col not specified. Making partition using _id.")
                self.curr_mapping['partition_col'] = ['_id']
                self.curr_mapping['partition_col_format'] = ['datetime']
            if(isinstance(self.curr_mapping['partition_col'], str)):
                self.curr_mapping['partition_col'] = [self.curr_mapping['partition_col']]
            
            if('partition_col_format' not in self.curr_mapping.keys()):
                self.curr_mapping['partition_col_format'] = ['str']
            if(isinstance(self.curr_mapping['partition_col_format'], str)):
                self.curr_mapping['partition_col_format'] = [self.curr_mapping['partition_col_format']]
            
            while(len(self.curr_mapping['partition_col']) > len(self.curr_mapping['partition_col_format'])):
                self.curr_mapping['partition_col_format'] = self.curr_mapping['partition_col_format'].append('str')
        
            for i in range(len(self.curr_mapping['partition_col'])):
                col = self.curr_mapping['partition_col'][i]
                col_form = self.curr_mapping['partition_col_format'][i]
                parq_col = "parquet_format_" + col
                if(col == 'migration_snapshot_date'):
                    self.curr_mapping['partition_col_format'][i] = 'datetime'
                    self.curr_mapping['fields'][col] = 'datetime'
                    self.partition_for_parquet.extend([parq_col + "_year", parq_col + "_month", parq_col + "_day", parq_col + "_hour"])
                    self.curr_mapping['fields'][parq_col + "_year"] = 'int'
                    self.curr_mapping['fields'][parq_col + "_month"] = 'int'
                    self.curr_mapping['fields'][parq_col + "_day"] = 'int'
                    self.curr_mapping['fields'][parq_col + "_hour"] = 'int'
                elif(col == '_id'):
                    self.partition_for_parquet.extend([parq_col + "_year", parq_col + "_month", parq_col + "_day", parq_col + "_hour"])
                    self.curr_mapping['fields'][parq_col + "_year"] = 'int'
                    self.curr_mapping['fields'][parq_col + "_month"] = 'int'
                    self.curr_mapping['fields'][parq_col + "_day"] = 'int'
                    self.curr_mapping['fields'][parq_col + "_hour"] = 'int'
                elif(col_form == 'str'):
                    self.partition_for_parquet.extend([parq_col])
                    self.curr_mapping['fields'][parq_col] = 'str'
                elif(col_form == 'int'):
                    self.partition_for_parquet.extend([parq_col])
                    self.curr_mapping['fields'][parq_col] = 'int'
                elif(col_form == 'float'):
                    self.partition_for_parquet.extend([parq_col])
                    self.curr_mapping['fields'][parq_col] = 'float'
                elif(col_form == 'datetime'):
                    self.partition_for_parquet.extend([parq_col + "_year", parq_col + "_month", parq_col + "_day", parq_col + "_hour"])
                    self.curr_mapping['fields'][parq_col + "_year"] = 'int'
                    self.curr_mapping['fields'][parq_col + "_month"] = 'int'
                    self.curr_mapping['fields'][parq_col + "_day"] = 'int'
                    self.curr_mapping['fields'][parq_col + "_hour"] = 'int'
                else:
                    raise UnrecognizedFormat(str(col_form) + ". Partition_col_format can be int, float, str or datetime")            

        self.saver = DMS_exporter(db = self.db, uid = self.curr_mapping['unique_id'], partition = self.partition_for_parquet)
        
    def process_data(self, start: int = 0, end: int = 0) -> Dict[str, Any]:
        docu_insert = []
        docu_update = []

        collection_encr = get_data_from_encr_db()
        all_documents = list(self.db_collection.find()[start:end])
        if(not all_documents or len(all_documents) == 0):
            return None
        for document in all_documents:
            insertion_time = utc_to_local(document['_id'].generation_time, self.tz_info)
            if('is_dump' in self.curr_mapping.keys() and self.curr_mapping['is_dump']):
                document['migration_snapshot_date'] = self.curr_run_cron_job
            if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):        
                for i in range(len(self.curr_mapping['partition_col'])):
                    col = self.curr_mapping['partition_col'][i]
                    col_form = self.curr_mapping['partition_col_format'][i]
                    parq_col = "parquet_format_" + col
                    if(col == '_id'):
                        document[parq_col + "_year"] = insertion_time.year
                        document[parq_col + "_month"] = insertion_time.month
                        document[parq_col + "_day"] = insertion_time.day
                        document[parq_col + "_hour"] = insertion_time.hour
                    elif(col_form == 'str'):
                        document[parq_col] = str(document[col])
                    elif(col_form == 'int'):
                        document[parq_col] = int(document[col])
                    elif(col_form == 'float'):
                        document[parq_col] = float(document[col])
                    elif(col_form == 'datetime'):
                        document[col] = convert_to_datetime(document[col], self.tz_info)
                        document[parq_col + "_year"] = document[col].year
                        document[parq_col + "_month"] = document[col].month
                        document[parq_col + "_day"] = document[col].day
                        document[parq_col + "_hour"] = document[col].hour
                    else:
                        raise UnrecognizedFormat(str(col_form) + ". Partition_col_format can be int, float, str or datetime")                    
            updation = False
            if('is_dump' not in self.curr_mapping.keys() or not self.curr_mapping['is_dump']):
                if(insertion_time < self.last_run_cron_job):
                    if('bookmark' in self.curr_mapping.keys() and self.curr_mapping['bookmark']):
                        if(self.curr_mapping['bookmark'] not in document.keys()):
                            document[self.curr_mapping['bookmark']] = None
                        docu_bookmark_date = convert_to_datetime(document[self.curr_mapping['bookmark']], self.tz_info)
                        if(docu_bookmark_date is not pd.Timestamp(None) and docu_bookmark_date <= self.last_run_cron_job):
                            continue
                        else:
                            updation = True
                    else:
                        document['_id'] = str(document['_id'])
                        encr = {
                            'collection': self.curr_mapping['unique_id'],
                            'map_id': document['_id'],
                            'document_sha': hashlib.sha256(json.dumps(document, default=str, sort_keys=True).encode()).hexdigest()
                        }
                        previous_records = collection_encr.find_one({'collection': self.curr_mapping['unique_id'], 'map_id': document['_id']})
                        if(previous_records):
                            if(previous_records['document_sha'] == encr['document_sha']):
                                continue
                            else:
                                updation = True
                                collection_encr.delete_one({'collection': self.curr_mapping['unique_id'], 'map_id': document['_id']})
                                collection_encr.insert_one(encr)
                        else:
                            collection_encr.insert_one(encr)
                else:
                    if('bookmark' not in self.curr_mapping.keys() or not self.curr_mapping['bookmark']):
                        document['_id'] = str(document['_id'])
                        encr = {
                            'collection': self.curr_mapping['unique_id'],
                            'map_id': document['_id'],
                            'document_sha': hashlib.sha256(json.dumps(document, default=str, sort_keys=True).encode()).hexdigest()
                        }
                        collection_encr.insert_one(encr)
            document = validate_or_convert(document, self.curr_mapping['fields'], self.tz_info)
            if(not updation):
                docu_insert.append(document)
            else:
                docu_update.append(document)
        ret_df_insert = typecast_df_to_schema(pd.DataFrame(docu_insert), self.curr_mapping['fields'])
        ret_df_update = typecast_df_to_schema(pd.DataFrame(docu_update), self.curr_mapping['fields'])
        return {'name': self.curr_mapping['collection_name'], 'df_insert': ret_df_insert, 'df_update': ret_df_update}

    def save_data(self, processed_collection: Dict[str, Any] = None) -> None:
        if(not processed_collection):
            return
        else:
            self.saver.save(processed_data = processed_collection, primary_keys = ['_id'])

    def process(self) -> None:
        self.get_data()
        self.inform("Data fetched.")
        self.preprocess()
        self.inform("Collection pre-processed.")
        start = 0
        while(True):    
            end = start + self.batch_size
            processed_collection = self.process_data(start=start, end=end)
            if(not processed_collection):
                self.inform("Processed all records from collection.")
                break
            self.save_data(processed_collection=processed_collection)
            time.sleep(self.time_delay)
            start += self.batch_size
        self.inform("Migration Complete.")
        if('is_dump' in self.curr_mapping.keys() and self.curr_mapping['is_dump'] and 'expiry' in self.curr_mapping.keys() and self.curr_mapping['expiry']):
            self.saver.expire(expiry = self.curr_mapping['expiry'], tz_info = self.tz_info)
            self.inform("Expired data removed.")
        self.saver.close()
        self.inform("Hope to see you again :')\n")
