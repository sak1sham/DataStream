from helper.util import validate_or_convert, convert_to_datetime, utc_to_local, typecast_df_to_schema, get_athena_dtypes
from db.encr_db import get_data_from_encr_db, get_last_run_cron_job
from helper.exceptions import *
from helper.logger import logger
from dst.main import DMS_exporter

from bson.objectid import ObjectId
from pymongo import MongoClient
import certifi
import pandas as pd
import datetime
import json
import hashlib
import pytz
from typing import List, Dict, Any
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

    def get_connectivity(self) -> int:
        try:
            client = MongoClient(self.db['source']['url'], tlsCAFile=certifi.where())
            database_ = client[self.db['source']['db_name']]
            self.db_collection = database_[self.curr_mapping['collection_name']]
            return self.db_collection.count_documents({})
        except Exception as e:
            self.err(e)
            self.db_collection = None
            raise ConnectionError("Unable to connect to source.")


    def fetch_data(self, start: int = 0, end: int = 0, query: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        if(query):
            return list(self.db_collection.find(query)[start:end])
        else:
            return list(self.db_collection.find()[start:end])


    def preprocess(self) -> None:
        if('fields' not in self.curr_mapping.keys()):
            self.curr_mapping['fields'] = {}
        
        self.last_run_cron_job = convert_to_datetime(get_last_run_cron_job(self.curr_mapping['unique_id']), self.tz_info)
        self.curr_run_cron_job = convert_to_datetime(datetime.datetime.utcnow(), self.tz_info)
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
                self.curr_mapping['partition_col_format'].append('str')
            
            for i in range(len(self.curr_mapping['partition_col'])):
                col = self.curr_mapping['partition_col'][i]
                col_form = self.curr_mapping['partition_col_format'][i]
                parq_col = "parquet_format_" + col
                if(col == 'migration_snapshot_date'):
                    self.curr_mapping['partition_col_format'][i] = 'datetime'
                    self.curr_mapping['fields'][col] = 'datetime'
                    self.partition_for_parquet.extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                    self.curr_mapping['fields'][parq_col + "_year"] = 'int'
                    self.curr_mapping['fields'][parq_col + "_month"] = 'int'
                    self.curr_mapping['fields'][parq_col + "_day"] = 'int'
                elif(col == '_id' and col_form == 'datetime'):
                    self.partition_for_parquet.extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                    self.curr_mapping['fields'][parq_col + "_year"] = 'int'
                    self.curr_mapping['fields'][parq_col + "_month"] = 'int'
                    self.curr_mapping['fields'][parq_col + "_day"] = 'int'
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
                    self.partition_for_parquet.extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                    self.curr_mapping['fields'][parq_col + "_year"] = 'int'
                    self.curr_mapping['fields'][parq_col + "_month"] = 'int'
                    self.curr_mapping['fields'][parq_col + "_day"] = 'int'
                else:
                    raise UnrecognizedFormat(str(col_form) + ". Partition_col_format can be int, float, str or datetime")
        
        if(self.curr_mapping['mode'] == 'dumping'):
            self.curr_mapping['fields']['migration_snapshot_date'] = 'datetime'

        self.saver = DMS_exporter(db = self.db, uid = self.curr_mapping['unique_id'], partition = self.partition_for_parquet)


    def add_partitions(self, document: Dict[str, Any], insertion_time) -> Dict[str, Any]:
        for i in range(len(self.curr_mapping['partition_col'])):
            col = self.curr_mapping['partition_col'][i]
            col_form = self.curr_mapping['partition_col_format'][i]
            parq_col = "parquet_format_" + col
            if(col == '_id'):
                document[parq_col + "_year"] = insertion_time.year
                document[parq_col + "_month"] = insertion_time.month
                document[parq_col + "_day"] = insertion_time.day
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
            else:
                raise UnrecognizedFormat(str(col_form) + ". Partition_col_format can be int, float, str or datetime")
        return document


    def dumping_data(self, start: int = 0, end: int = 0) -> Dict[str, Any]:
        ## When we are dumping data, snapshots of the datastore are captured at regular intervals inside destination
        ## We insert the snapshots, and no updation is performed
        ## All the records are inserted, there is no need for bookmarks
        ## By default, we add a column 'migration_snapshot_date' to capture the starting time of migration
        docu_insert = []
        migration_start_id = ObjectId.from_datetime(self.curr_run_cron_job)
        query = {"_id": {"$lt": migration_start_id}}
        all_documents = self.fetch_data(start=start, end=end, query=query)
        if(not all_documents or len(all_documents) == 0):
            return None
        for document in all_documents:
            insertion_time = utc_to_local(document['_id'].generation_time, self.tz_info)
            document['migration_snapshot_date'] = self.curr_run_cron_job
            if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):
                document = self.add_partitions(document=document, insertion_time=insertion_time)        
            document = validate_or_convert(document, self.curr_mapping['fields'], self.tz_info)
            docu_insert.append(document)
        ret_df_insert = typecast_df_to_schema(pd.DataFrame(docu_insert), self.curr_mapping['fields'])
        dtypes = get_athena_dtypes(self.curr_mapping['fields'])
        return {'name': self.curr_mapping['collection_name'], 'df_insert': ret_df_insert, 'df_update': pd.DataFrame({}), 'dtypes': dtypes}


    def adding_new_data(self, start: int = 0, end: int = 0, mode: str = 'logging') -> Dict[str, Any]:
        ## Only insertions are captured from source and sent to destination.
        ## It's assumed that no updation is performed
        ## bookmark_creation is required, which in case of mongo is '_id'
        ## If bookmark_updation is not provided, and the mode is syncing, then hashes are stored
        docu_insert = []
        collection_encr = get_data_from_encr_db()
        migration_start_id = ObjectId.from_datetime(self.curr_run_cron_job)
        migration_prev_id = ObjectId.from_datetime(self.last_run_cron_job)
        query = {
            "_id": {
                "$gte": migration_prev_id, 
                "$lt": migration_start_id
            }
        }
        all_documents = self.fetch_data(start=start, end=end, query=query)
        if(not all_documents or len(all_documents) == 0):
            return None
        for document in all_documents:
            insertion_time = utc_to_local(document['_id'].generation_time, self.tz_info)
            if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):
                document = self.add_partitions(document=document, insertion_time=insertion_time)
            if(mode == 'syncing' and ('bookmark' not in self.curr_mapping.keys() or not self.curr_mapping['bookmark'])):
                ## if updation bookmark is not present, we need to store hashes to check for updates later
                document['_id'] = str(document['_id'])
                encr = {
                    'collection': self.curr_mapping['unique_id'],
                    'map_id': document['_id'],
                    'document_sha': hashlib.sha256(json.dumps(document, default=str, sort_keys=True).encode()).hexdigest()
                }
                collection_encr.insert_one(encr)
            document = validate_or_convert(document, self.curr_mapping['fields'], self.tz_info)
            docu_insert.append(document)
        ret_df_insert = typecast_df_to_schema(pd.DataFrame(docu_insert), self.curr_mapping['fields'])
        dtypes = get_athena_dtypes(self.curr_mapping['fields'])
        return {'name': self.curr_mapping['collection_name'], 'df_insert': ret_df_insert, 'df_update': pd.DataFrame({}), 'dtypes': dtypes}


    def updating_data(self, start: int = 0, end: int = 0, improper_bookmarks: bool = False) -> Dict[str, Any]:
        ## When we are updating data, no need to capture new records, only old records are to be processed
        ## If bookmark for updation is not provided, we can ignore it, and filter it further through encryption
        ## Both bookmarks are otherwise required 
        docu_update = []
        collection_encr = get_data_from_encr_db()
        migration_start_id = ObjectId.from_datetime(self.curr_run_cron_job)
        migration_prev_id = ObjectId.from_datetime(self.last_run_cron_job)
        all_documents = []
        if('bookmark' in self.curr_mapping.keys() and self.curr_mapping['bookmark'] and not improper_bookmarks):
            query = {
                "$and":[
                    {
                        self.curr_mapping['bookmark']: {
                            "$gte": migration_prev_id,
                            "$lt": migration_start_id
                        }
                    },
                    {   "_id": {
                            "$lt": migration_prev_id,
                        }
                    }
                ]
            }
            all_documents = self.fetch_data(start=start, end=end, query=query)
        else:
            query = {
                "_id": {
                    "$lt": migration_prev_id,
                }
            }
            all_documents = self.fetch_data(start=start, end=end, query=query)
        if(not all_documents or len(all_documents) == 0):
            return None
        for document in all_documents:
            insertion_time = utc_to_local(document['_id'].generation_time, self.tz_info)
            if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):        
                document = self.add_partitions(document=document, insertion_time=insertion_time)

            if('bookmark' in self.curr_mapping.keys() and self.curr_mapping['bookmark'] and improper_bookmarks):
                if(self.curr_mapping['bookmark'] not in document.keys()):
                    document[self.curr_mapping['bookmark']] = None
                docu_bookmark_date = convert_to_datetime(document[self.curr_mapping['bookmark']], self.tz_info)
                ## if docu_bookmark_date is None, that means the document was never updated
                if(docu_bookmark_date is not pd.Timestamp(None) and docu_bookmark_date < self.last_run_cron_job):
                    continue
            elif('bookmark' not in self.curr_mapping.keys() or not self.curr_mapping['bookmark']):
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
                        collection_encr.delete_one({'collection': self.curr_mapping['unique_id'], 'map_id': document['_id']})
                        collection_encr.insert_one(encr)
                else:
                    collection_encr.insert_one(encr)
            
            document = validate_or_convert(document, self.curr_mapping['fields'], self.tz_info)
            docu_update.append(document)
        ret_df_update = typecast_df_to_schema(pd.DataFrame(docu_update), self.curr_mapping['fields'])
        dtypes = get_athena_dtypes(self.curr_mapping['fields'])
        return {'name': self.curr_mapping['collection_name'], 'df_insert': pd.DataFrame({}), 'df_update': ret_df_update, 'dtypes': dtypes}


    def save_data(self, processed_collection: Dict[str, Any] = None) -> None:
        if(not processed_collection):
            return
        else:
            if('name' not in processed_collection.keys()):
                processed_collection['name'] = self.curr_mapping['collection_name']
            if('df_insert' not in processed_collection.keys()):
                processed_collection['df_insert'] = pd.DataFrame({})
            if('df_update' not in processed_collection.keys()):
                processed_collection['df_update'] = pd.DataFrame({})
            primary_keys = []
            if(self.curr_mapping['mode'] != 'dumping'):
                primary_keys = ['_id']
            self.saver.save(processed_data = processed_collection, primary_keys = primary_keys)


    def process(self) -> None:
        max_end = self.get_connectivity()
        self.inform("Data fetched.")
        self.preprocess()
        self.inform("Collection pre-processed.")
        start = 0
        already_done_with_insertion = False
        updated_in_destination = True
        processed_collection = {}
        processed_collection_u = {}
        end = 0

        while(True):
            end = start + self.batch_size
            if(self.curr_mapping['mode'] == 'dumping'):
                processed_collection = self.dumping_data(start=start, end=end)
            elif(self.curr_mapping['mode'] == 'logging'):
                processed_collection = self.adding_new_data(start=start, end=end, mode='logging')
            elif(self.curr_mapping['mode'] == 'syncing'):
                if(not already_done_with_insertion):
                    processed_collection = self.adding_new_data(start=start, end=end, mode='syncing')
                else:
                    if('improper_bookmarks' not in self.curr_mapping.keys()):
                        self.curr_mapping['improper_bookmarks'] = True
                    processed_collection_u = {}
                    processed_collection_u = self.updating_data(start=start, end=end, improper_bookmarks=self.curr_mapping['improper_bookmarks'])
                    if(processed_collection_u):
                        if(not updated_in_destination):
                            processed_collection['df_update'] = typecast_df_to_schema(processed_collection['df_update'].append([processed_collection_u['df_update']]), self.curr_mapping['fields'])
                        else:
                            processed_collection['df_update'] = processed_collection_u['df_update']
                        self.inform("Found " + str(processed_collection['df_update'].shape[0]) + " updations upto now.")
            else:
                raise IncorrectMapping("migration mode can either be \'dumping\', \'logging\' or \'syncing\'")
            if(not processed_collection):
                if(self.curr_mapping['mode'] == 'syncing' and not already_done_with_insertion):
                    already_done_with_insertion = True
                    self.inform("Inserted all records from collection, looking forward to updations.")
                    start = 0
                    end = 0
                    processed_collection = {}
                    continue
                else:
                    self.inform("Processing complete.")
                    break
            else:
                if(self.curr_mapping['mode'] == 'dumping' or self.curr_mapping['mode'] == 'logging' or (self.curr_mapping['mode'] == 'syncing' and not already_done_with_insertion)):
                    self.save_data(processed_collection=processed_collection)
                    processed_collection = {}
                elif(processed_collection['df_update'].shape[0] >= self.batch_size):
                    self.save_data(processed_collection=processed_collection)
                    updated_in_destination = True
                    processed_collection = {}
                elif((not self.curr_mapping['improper_bookmarks'] and not processed_collection_u) or (self.curr_mapping['improper_bookmarks'] and end >= max_end)):
                    self.save_data(processed_collection=processed_collection)
                    updated_in_destination = True
                    break
                else:
                    updated_in_destination = False
                    ## Still not saved the updates, will update together a large number of records...will save time

            time.sleep(self.time_delay)
            start += self.batch_size

        self.inform("Migration Complete.")
        if(self.curr_mapping['mode'] == 'dumping' and 'expiry' in self.curr_mapping.keys() and self.curr_mapping['expiry']):
            self.saver.expire(expiry = self.curr_mapping['expiry'], tz_info = self.tz_info)
            self.inform("Expired data removed.")
        self.saver.close()
        self.inform("Hope to see you again :')\n")
 