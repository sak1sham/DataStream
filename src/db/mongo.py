from helper.util import validate_or_convert, convert_to_datetime, utc_to_local, typecast_df_to_schema, get_athena_dtypes
from db.encr_db import get_data_from_encr_db, get_last_run_cron_job, save_recovery_data, set_last_run_cron_job, get_last_migrated_record, set_last_migrated_record, save_recovery_data, get_recovery_data, delete_recovery_data, get_job_records, save_job_data, get_job_mb
from helper.exceptions import *
from helper.logger import logger
from dst.main import DMS_exporter
from helper.sigterm import GracefulKiller, NormalKiller
from notifications.slack_notify import send_message
from config.settings import settings

from bson.objectid import ObjectId
from pymongo import MongoClient
import certifi
import pandas as pd
import datetime
import json
import hashlib
import pytz
from typing import List, Dict, Any, Tuple
import time

'''
    Dictionary returned after processing data contains following fields:
        processed_collection = {
            'name': str: collection_name,
            'df_insert': pd.Dataframe({}): all records to be inserted
            'df_update': pd.DataFrame({}): all records to be updated,
            'dtypes': Dict[str, str]: Glue/Athena mapping of all fields
        }
'''


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
        self.n_insertions = 0
        self.n_updations = 0
        self.prev_n_records = 0
        self.start_time = datetime.datetime.utcnow()
        self.curr_megabytes_processed = 0


    def inform(self, message: str = None, save: bool = False) -> None:
        logger.inform(job_id= self.curr_mapping['unique_id'], s=(self.curr_mapping['unique_id'] + ": " + message), save=save)

    def warn(self, message: str = None) -> None:
        logger.warn(job_id=self.curr_mapping['unique_id'], s=(self.curr_mapping['unique_id'] + ": " + message))

    def err(self, error: Any = None) -> None:
        logger.err(job_id=self.curr_mapping['unique_id'], s=error)


    def get_connectivity(self) -> None:
        '''
            Makes the connection to the MongoDb database and returns the number of documents present inside the collection
        '''
        try:
            certificate = certifi.where()
            if('certificate_file' in self.db['source'].keys() and self.db['source']['certificate_file']):
                certificate = "config/" + self.db['source']['certificate_file']
            client = MongoClient(self.db['source']['url'], tlsCAFile=certificate)
            database_ = client[self.db['source']['db_name']]
            self.db_collection = database_[self.curr_mapping['collection_name']]
        except Exception as e:
            self.err(error = e)
            self.db_collection = None
            raise ConnectionError("Unable to connect to source.")


    def fetch_data(self, start: int = -1, end: int = -1, query: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        '''
            This functions returns the list of all documents inside the collection, or when the query (mongo query) is executed
                1. All records are sorted as per date of creation
                2. start and end specify the starting and ending of records to be returned
                3. if start and end are not specified, self.batch_size is used to return some records from beginning
        '''
        if(query):
            if(start == -1 and end == -1):
                return list(self.db_collection.find(query).sort("_id", 1).limit(self.batch_size))
            else:
                return list(self.db_collection.find(query).sort("_id", 1)[start:end])
        else:
            if(start == -1 and end == -1):
                return list(self.db_collection.find().sort("_id", 1).limit(self.batch_size))
            else:
                return list(self.db_collection.find().sort("_id", 1)[start:end])
            

    def preprocess(self) -> None:
        '''
            This function handles all the preprocessing steps.
                1. If partitions need to be made, store separate partition fields and their datatypes in the job-mapping.
                2. If data is to be dumped, add a field 'migration_snapshot_date' in job-mapping with format as datetime
                3. Create a saver object to save data at destination.
        '''
        if('fields' not in self.curr_mapping.keys()):
            self.curr_mapping['fields'] = {}
        
        self.last_run_cron_job = get_last_run_cron_job(self.curr_mapping['unique_id'])
        self.curr_run_cron_job = pytz.utc.localize(datetime.datetime.utcnow())

        self.partition_for_parquet = []
        if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):
            if('partition_col' not in self.curr_mapping.keys() or not self.curr_mapping['partition_col']):
                self.warn(message="Partition_col not specified. Making partition using _id.")
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
        
        if(self.curr_mapping['mode'] == 'dumping' or self.curr_mapping['mode'] == 'mirroring'):
            self.curr_mapping['fields']['migration_snapshot_date'] = 'datetime'

        mirroring = (self.curr_mapping['mode'] == 'mirroring')
        self.saver = DMS_exporter(db = self.db, uid = self.curr_mapping['unique_id'], partition = self.partition_for_parquet, mirroring=mirroring, table_name=self.curr_mapping['collection_name'])        

        if(self.last_run_cron_job > datetime.datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=pytz.utc)):
            self.prev_n_records = get_job_records(self.curr_mapping['unique_id'])
            if(not self.prev_n_records):
                self.prev_n_records = self.saver.count_n_records(table_name = self.curr_mapping['collection_name'])
        else:
            self.prev_n_records = get_job_records(self.curr_mapping['unique_id'])
            if(not self.prev_n_records):
                self.prev_n_records = 0


    def save_job_working_data(self) -> None:
        self.curr_megabytes_processed = self.curr_megabytes_processed/1e6
        total_records = self.prev_n_records + self.n_insertions
        total_time = (datetime.datetime.utcnow() - self.start_time).total_seconds()
        total_megabytes = 0
        if (self.n_insertions + self.n_updations > 0):
            total_megabytes = (self.curr_megabytes_processed/(self.n_insertions + self.n_updations))*total_records
        else:
            total_megabytes = get_job_mb(self.curr_mapping['unique_id'])
        job_data = {
            "job_id": self.curr_mapping['unique_id'],
            "table_name": self.curr_mapping['collection_name'],
            "insertions": self.n_insertions,
            "updations": self.n_updations,
            "total_records": total_records,
            "start_time": self.start_time,
            "total_time": total_time,
            "curr_megabytes_processed": self.curr_megabytes_processed,
            "total_megabytes": total_megabytes,
        }
        save_job_data(job_data)
        self.inform("Saved data for job to be shown on dashboard.")



    def postprocess(self):
        '''
            After all migration has been performed, we save the datetime of this job. This helps in finding all records updated after this datetime, and before next job is running.
        '''
        self.inform(message = "Inserted " + str(self.n_insertions) + " records")
        self.inform(message = "Updated " + str(self.n_updations) + " records")
        set_last_run_cron_job(job_id = self.curr_mapping['unique_id'], timing = self.curr_run_cron_job)
        delete_recovery_data(job_id=self.curr_mapping['unique_id'])


    def add_partitions(self, document: Dict[str, Any], insertion_time) -> Dict[str, Any]:
        '''
            If partitions are specified in mapping, add partition fields to the documents.
        '''
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
                document[col] = convert_to_datetime(document[col], pytz.utc)
                document[parq_col + "_year"] = str(float(document[col].year))
                document[parq_col + "_month"] = str(float(document[col].month))
                document[parq_col + "_day"] = str(float(document[col].day))
            else:
                raise UnrecognizedFormat(str(col_form) + ". Partition_col_format can be int, float, str or datetime")
        return document


    def dumping_data(self, start: int = 0, end: int = 0) -> Dict[str, Any]:
        '''
            When we are dumping/mirroring data, snapshots of the datastore are captured at regular intervals inside destination
            We insert the snapshots, and no updation is performed
            All the records are inserted, there is no need for bookmarks
            By default, we add a column 'migration_snapshot_date' to capture the starting time of migration
        '''
        docu_insert = []
        migration_start_id = ObjectId.from_datetime(self.curr_run_cron_job)
        query = {"_id": {"$lt": migration_start_id}}
        all_documents = self.fetch_data(start=start, end=end, query=query)
        if(not all_documents or len(all_documents) == 0):
            return None
        for document in all_documents:
            insertion_time = document['_id'].generation_time
            document['migration_snapshot_date'] = self.curr_run_cron_job
            if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):
                document = self.add_partitions(document=document, insertion_time=insertion_time)        
            document = validate_or_convert(document, self.curr_mapping['fields'], pytz.utc)
            docu_insert.append(document)
        ret_df_insert = typecast_df_to_schema(pd.DataFrame(docu_insert), self.curr_mapping['fields'])
        dtypes = get_athena_dtypes(self.curr_mapping['fields'])
        return {'name': self.curr_mapping['collection_name'], 'df_insert': ret_df_insert, 'df_update': pd.DataFrame({}), 'dtypes': dtypes}        


    def adding_new_data(self, mode: str = 'logging') -> Dict[str, Any]:
        '''
            Function to find new data in the collection and return after processing it. Final result is a dataframe in the required data type format
                1. Only insertions are captured from source and sent to destination.
                2. It's assumed that no updation is performed
                3. '_id' field is used to identify when the record was created
                4. If bookmark (to check for updation) is not provided, and the mode is syncing, then hashes are stored in order to check for updations later
            Returns None if no more batches need to be searched for. Otherwise, returns a dictionary (Dict[str, Any]) of data to be saved with following keys:
            processed_collection = {
                'name': str: collection_name,
                'df_insert': pd.Dataframe({}): all records to be inserted
                'df_update': pd.DataFrame({}): all records to be updated,
                'dtypes': Dict[str, str]: Glue/Athena mapping of all fields
            }
        '''
        migration_start_id = ObjectId.from_datetime(self.curr_run_cron_job)
        migration_prev_id = ObjectId.from_datetime(self.last_run_cron_job)
        rec = get_last_migrated_record(self.curr_mapping['unique_id'])
        if(rec):
            ## If we know till which record was the last migration performed, then we can filter new records easily
            migration_prev_id = rec['record_id']
        
        docu_insert = []
        collection_encr = get_data_from_encr_db()
        all_documents = []

        self.inform(message = 'Inserting some records created after ' + str(migration_prev_id.generation_time) + '.')
        query = {
            "_id": {
                "$gt": migration_prev_id, 
                "$lte": migration_start_id
            }
        }
        all_documents = self.fetch_data(query=query)
        if(not all_documents or len(all_documents) == 0):
            return None
        ## all_documents is a list of documents, each document as a dictionary datatype. 
        ## The documents are all sorted as per creation_date and belong to a particular batch (self.batch_size)
        
        for document in all_documents:
            insertion_time = document['_id'].generation_time
            if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):
                ## If data needs to be stored in partitioned manner at destination, we need to add some partition fields to each document
                document = self.add_partitions(document=document, insertion_time=insertion_time)
            if(mode == 'syncing' and ('bookmark' not in self.curr_mapping.keys() or not self.curr_mapping['bookmark'])):
                ## if updation bookmark (for example - 'updated_at', or 'modified_at') is not present, we need to store hashes as metadata to check for updates later
                document['_id'] = str(document['_id'])
                encr = {
                    'collection': self.curr_mapping['unique_id'],
                    'map_id': document['_id'],
                    'document_sha': hashlib.sha256(json.dumps(document, default=str, sort_keys=True).encode()).hexdigest()
                }
                collection_encr.insert_one(encr)
            ## All pre-requisite processing has been done. Next step is to convert all documents (dictionaries) into destined data-types.
            document = validate_or_convert(document, self.curr_mapping['fields'], pytz.utc)
            docu_insert.append(document)
        ## All fields in the document have been individually converted to destined data-types.
        ## Next and final processing step is to convert the documents into a dataframe, and type-cast the dataframe as a whole to destined data-types (as a double-check)
        ret_df_insert = typecast_df_to_schema(pd.DataFrame(docu_insert), self.curr_mapping['fields'])
        ## Processing is complete. While saving, we need to pass the datatypes of columns to Athena (to create SQL table).
        dtypes = get_athena_dtypes(self.curr_mapping['fields'])
        return {'name': self.curr_mapping['collection_name'], 'df_insert': ret_df_insert, 'df_update': pd.DataFrame({}), 'dtypes': dtypes}


    def updating_recovered_data(self, start: int = 0, end: int = 0, improper_bookmarks: bool = False, last_recovered_record: Any = None, recovered_data_timing: datetime.datetime = None) -> Dict[str, Any]:
        '''
            This function finds all records which are inserted in last run, but the last run failed due to any reason, and finds and migrates those records which were updated at the source later
                1. When we are updating data, no need to capture new records, only old records are to be processed
                2. If bookmark for updation is not provided, we can find all records, and filter them further while matching with their encryptions
            Returns None if no more batches need to be searched for. Otherwise, returns a dictionary (Dict[str, Any]) of data to be saved with following keys:
            processed_collection = {
                'name': str: collection_name,
                'df_insert': pd.Dataframe({}): all records to be inserted
                'df_update': pd.DataFrame({}): all records to be updated,
                'dtypes': Dict[str, str]: Glue/Athena mapping of all fields
            }
        '''
        docu_update = []
        collection_encr = get_data_from_encr_db()
        migration_prev_id = ObjectId.from_datetime(self.last_run_cron_job - datetime.timedelta(minutes=1))
        self.inform(message = 'Updating all records updated between ' + str(self.last_run_cron_job) + " and " + str(self.curr_run_cron_job))
        all_documents = []
        ## Bookmark is that field in the document (dictionary) which identifies the timestamp when the record was updated
        ## If bookmark field is not proper (can either be string, or integer timestamp, or datetime), we set improper_bookmarks as True, and can't query inside mongodb collection using that field directly
        ## In case of improper_bookmarks = True, we need to convert that field into a datetime.datetime datatype and then compare
        if('bookmark' in self.curr_mapping.keys() and self.curr_mapping['bookmark'] and not improper_bookmarks):
            ## Return all documents which are greater than and equal to ('$gte') the time when last job was performed
            ## and less than ('$lt') the starting time of current job
            ## Also, the record shall be created before the last job ended (i.e., it should already be migrated and present in destination)
            query = {
                "$and":[
                    {
                        self.curr_mapping['bookmark']: {
                            "$gt": recovered_data_timing,
                            "$lte": self.curr_run_cron_job,
                        }
                    },
                    {   "_id": {
                            "$gt": migration_prev_id,
                            "$lte": last_recovered_record
                        }
                    }
                ]
            }
            all_documents = self.fetch_data(start=start, end=end, query=query)
        else:
            ## If we are not able to do the comparison of when the document was updated,
            ## we fetch all documents migrated until the last job, and filter them in further processing steps
            query = {
                "_id": {
                    "$gt": migration_prev_id,
                    "$lte": last_recovered_record
                }
            }
            all_documents = self.fetch_data(start=start, end=end, query=query)
        if(not all_documents or len(all_documents) == 0):
            return None
        ## all_documents is a list of documents, each document as a dictionary datatype. 
        ## The documents are all sorted as per creation_date and belong to a particular batch (self.batch_size)

        for document in all_documents:
            insertion_time = document['_id'].generation_time
            if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):        
                ## If data needs to be stored in partitioned manner at destination, we need to add some partition fields to each document
                document = self.add_partitions(document=document, insertion_time=insertion_time)

            if('bookmark' in self.curr_mapping.keys() and self.curr_mapping['bookmark'] and improper_bookmarks):
                if(self.curr_mapping['bookmark'] not in document.keys()):
                    document[self.curr_mapping['bookmark']] = None
                docu_bookmark_date = convert_to_datetime(document[self.curr_mapping['bookmark']], pytz.utc)
                ## if docu_bookmark_date is None, that means the document was never updated
                if(docu_bookmark_date is not pd.Timestamp(None) and docu_bookmark_date <= recovered_data_timing):
                    continue
            elif('bookmark' not in self.curr_mapping.keys() or not self.curr_mapping['bookmark']):
                ## if bookmark is not present, we need to do the encryption of the document
                ## if the encryption of document is different when compared to its previous hash (already stored in metadata) that means the document has been modified
                ## otherwise, no updation are performed in that document
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
            
            document = validate_or_convert(document, self.curr_mapping['fields'], pytz.utc)
            docu_update.append(document)
        ret_df_update = typecast_df_to_schema(pd.DataFrame(docu_update), self.curr_mapping['fields'])
        dtypes = get_athena_dtypes(self.curr_mapping['fields'])
        return {'name': self.curr_mapping['collection_name'], 'df_insert': pd.DataFrame({}), 'df_update': ret_df_update, 'dtypes': dtypes}


    def updating_data(self, start: int = 0, end: int = 0, improper_bookmarks: bool = False) -> Dict[str, Any]:
        '''
            This function finds all records which are updated at the source, and accordingly returns a set of records to be overwritten at destination.
                1. When we are updating data, no need to capture new records, only old records are to be processed
                2. If bookmark for updation is not provided, we can find all records, and filter them further while matching with their encryptions
            Returns None if no more batches need to be searched for. Otherwise, returns a dictionary (Dict[str, Any]) of data to be saved with following keys:
            processed_collection = {
                'name': str: collection_name,
                'df_insert': pd.Dataframe({}): all records to be inserted
                'df_update': pd.DataFrame({}): all records to be updated,
                'dtypes': Dict[str, str]: Glue/Athena mapping of all fields
            }
        '''
        docu_update = []
        collection_encr = get_data_from_encr_db()
        migration_prev_id = ObjectId.from_datetime(self.last_run_cron_job + datetime.timedelta(minutes=1))
        self.inform(message = 'Updating all records updated between ' + str(self.last_run_cron_job) + " and " + str(self.curr_run_cron_job))
        all_documents = []
        ## Bookmark is that field in the document (dictionary) which identifies the timestamp when the record was updated
        ## If bookmark field is not proper (can either be string, or integer timestamp, or datetime), we set improper_bookmarks as True, and can't query inside mongodb collection using that field directly
        ## In case of improper_bookmarks = True, we need to convert that field into a datetime.datetime datatype and then compare
        last_run_c_job = self.last_run_cron_job
        if('grace_updation_lag' in self.curr_mapping.keys() and self.curr_mapping['grace_updation_lag']):
            '''
                parameter to pass to double-check for any updations missed.
            '''
            days = 0
            hours = 0
            minutes = 0
            if('days' in self.curr_mapping['grace_updation_lag'].keys()):
                days = self.curr_mapping['grace_updation_lag']['days']
            if('hours' in self.curr_mapping['grace_updation_lag'].keys()):
                hours = self.curr_mapping['grace_updation_lag']['hours']
            if('minutes' in self.curr_mapping['grace_updation_lag'].keys()):
                minutes = self.curr_mapping['grace_updation_lag']['minutes']
            last_run_c_job = last_run_c_job - datetime.timedelta(days=days, hours=hours, minutes=minutes)
        
        if('bookmark' in self.curr_mapping.keys() and self.curr_mapping['bookmark'] and not improper_bookmarks):
            ## Return all documents which are greater than and equal to ('$gte') the time when last job was performed
            ## and less than ('$lt') the starting time of current job
            ## Also, the record shall be created before the last job ended (i.e., it should already be migrated and present in destination)
            query = {
                "$and":[
                    {
                        self.curr_mapping['bookmark']: {
                            "$gt": last_run_c_job,
                            "$lte": self.curr_run_cron_job,
                        }
                    },
                    {   "_id": {
                            "$lte": migration_prev_id,
                        }
                    }
                ]
            }
            all_documents = self.fetch_data(start=start, end=end, query=query)
        else:
            ## If we are not able to do the comparison of when the document was updated,
            ## we fetch all documents migrated until the last job, and filter them in further processing steps
            query = {
                "_id": {
                    "$lte": migration_prev_id,
                }
            }
            all_documents = self.fetch_data(start=start, end=end, query=query)
        if(not all_documents or len(all_documents) == 0):
            return None
        ## all_documents is a list of documents, each document as a dictionary datatype. 
        ## The documents are all sorted as per creation_date and belong to a particular batch (self.batch_size)

        for document in all_documents:
            insertion_time = document['_id'].generation_time
            if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):        
                ## If data needs to be stored in partitioned manner at destination, we need to add some partition fields to each document
                document = self.add_partitions(document=document, insertion_time=insertion_time)

            if('bookmark' in self.curr_mapping.keys() and self.curr_mapping['bookmark'] and improper_bookmarks):
                if(self.curr_mapping['bookmark'] not in document.keys()):
                    document[self.curr_mapping['bookmark']] = None
                docu_bookmark_date = convert_to_datetime(document[self.curr_mapping['bookmark']], pytz.utc)
                ## if docu_bookmark_date is None, that means the document was never updated
                if(docu_bookmark_date is not pd.Timestamp(None) and docu_bookmark_date <= last_run_c_job):
                    continue
            elif('bookmark' not in self.curr_mapping.keys() or not self.curr_mapping['bookmark']):
                ## if bookmark is not present, we need to do the encryption of the document
                ## if the encryption of document is different when compared to its previous hash (already stored in metadata) that means the document has been modified
                ## otherwise, no updation are performed in that document
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
            
            document = validate_or_convert(document, self.curr_mapping['fields'], pytz.utc)
            docu_update.append(document)
        ret_df_update = typecast_df_to_schema(pd.DataFrame(docu_update), self.curr_mapping['fields'])
        dtypes = get_athena_dtypes(self.curr_mapping['fields'])
        return {'name': self.curr_mapping['collection_name'], 'df_insert': pd.DataFrame({}), 'df_update': ret_df_update, 'dtypes': dtypes}


    def buffer_updating_data(self, start: int = 0, end: int = 0, improper_bookmarks: bool = False, buffer_days: int = 0, buffer_hours: int = 0, buffer_minutes: int = 0) -> Dict[str, Any]:
        '''
            This function finds all records which are updated at the source, and but their bookmark changed later. It returns a set of records to be overwritten at destination.
                1. When we are updating data, no need to capture new records, only old records are to be processed
                2. If bookmark for updation is not provided, we can find all records, and filter them further while matching with their encryptions
            Returns None if no more batches need to be searched for. Otherwise, returns a dictionary (Dict[str, Any]) of data to be saved with following keys:
            processed_collection = {
                'name': str: collection_name,
                'df_insert': pd.Dataframe({}): all records to be inserted
                'df_update': pd.DataFrame({}): all records to be updated,
                'dtypes': Dict[str, str]: Glue/Athena mapping of all fields
            }
        '''
        docu_update = []
        collection_encr = get_data_from_encr_db()
        check_time1 = ObjectId.from_datetime(self.curr_run_cron_job - datetime.timedelta(days=buffer_days, hours=buffer_hours, minutes=buffer_minutes))
        check_time2 = ObjectId.from_datetime(self.curr_run_cron_job)
        self.inform(message = 'Updating all records updated between {0} and {1}'.format(check_time1.generation_time, check_time2.generation_time))
        all_documents = []
        if('bookmark' in self.curr_mapping.keys() and self.curr_mapping['bookmark'] and not improper_bookmarks):
            ## Return all documents which are greater than and equal to ('$gte') the time when last job was performed
            ## and less than ('$lt') the starting time of current job
            ## Also, the record shall be created before the last job ended (i.e., it should already be migrated and present in destination)
            query = {
                "$and":[
                    {
                        self.curr_mapping['bookmark']: {
                            "$gt": check_time1.generation_time,
                            "$lte": check_time2.generation_time,
                        }
                    },
                    {   "_id": {
                            "$lte": get_last_migrated_record(self.curr_mapping['unique_id'])['record_id'],
                        }
                    }
                ]
            }
            all_documents = self.fetch_data(start=start, end=end, query=query)
        else:
            ## If we are not able to do the comparison of when the document was updated,
            ## we fetch all documents migrated until the last job, and filter them in further processing steps
            query = {
                "_id": {
                    "$lte": get_last_migrated_record(self.curr_mapping['unique_id'])['record_id'],
                }
            }
            all_documents = self.fetch_data(start=start, end=end, query=query)
        if(not all_documents or len(all_documents) == 0):
            return None
        ## all_documents is a list of documents, each document as a dictionary datatype. 
        ## The documents are all sorted as per creation_date and belong to a particular batch (self.batch_size)

        for document in all_documents:
            insertion_time = document['_id'].generation_time
            if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):        
                ## If data needs to be stored in partitioned manner at destination, we need to add some partition fields to each document
                document = self.add_partitions(document=document, insertion_time=insertion_time)

            if('bookmark' in self.curr_mapping.keys() and self.curr_mapping['bookmark'] and improper_bookmarks):
                if(self.curr_mapping['bookmark'] not in document.keys()):
                    document[self.curr_mapping['bookmark']] = None
                docu_bookmark_date = convert_to_datetime(document[self.curr_mapping['bookmark']], pytz.utc)
                ## if docu_bookmark_date is None, that means the document was never updated
                if(docu_bookmark_date is not pd.Timestamp(None) and docu_bookmark_date < check_time1.generation_time):
                    continue
            elif('bookmark' not in self.curr_mapping.keys() or not self.curr_mapping['bookmark']):
                ## if bookmark is not present, we need to do the encryption of the document
                ## if the encryption of document is different when compared to its previous hash (already stored in metadata) that means the document has been modified
                ## otherwise, no updation are performed in that document
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
            
            document = validate_or_convert(document, self.curr_mapping['fields'], pytz.utc)
            docu_update.append(document)
        ret_df_update = typecast_df_to_schema(pd.DataFrame(docu_update), self.curr_mapping['fields'])
        dtypes = get_athena_dtypes(self.curr_mapping['fields'])
        return {'name': self.curr_mapping['collection_name'], 'df_insert': pd.DataFrame({}), 'df_update': ret_df_update, 'dtypes': dtypes}


    def dumping_process(self) -> None:
        '''
            DUMPING: Everytime the job runs, entire data present in the collection is migrated to destination,
            Multiple copies of the same data are created.
            This function handles the dumping/mirroring process.
            While dumping, we add another column 'migration_snapshot_date' that indicates the datetime when migration was performed.
        '''
        start = 0
        processed_collection = {}
        end = 0
        while(True):
            end = start + self.batch_size
            processed_collection = self.dumping_data(start=start, end=end)
            if(not processed_collection):
                self.inform(message = "Processing complete.")
                break
            else:
                self.save_data(processed_collection=processed_collection)
                processed_collection = {}
            time.sleep(self.time_delay)
        self.inform(message = "Migration Complete.", save=True)
        if('expiry' in self.curr_mapping.keys() and self.curr_mapping['expiry']):
            self.saver.expire(expiry = self.curr_mapping['expiry'], tz_info = self.tz_info)
            self.inform(message = "Expired data removed.", save=True)


    def logging_process(self) -> None:
        '''
            Function to handle logging process.
            LOGGING: We assume no updations are performed at source, and if performed, the updates need not be migrated to destination
            Only newly inserted records are migrated
        '''
        processed_collection = {}
        while(True):
            processed_collection = self.adding_new_data(mode='logging')
            if(not processed_collection):
                self.inform(message = "Processing complete.")
                break
            else:
                killer = NormalKiller()
                if(self.curr_mapping['cron'] == 'self-managed'):
                    killer = GracefulKiller()
                while not killer.kill_now:
                    self.save_data(processed_collection=processed_collection)
                    if(processed_collection['df_insert'].shape[0]):
                        last_record_id = ObjectId(processed_collection['df_insert']['_id'].iloc[-1])
                        set_last_migrated_record(job_id = self.curr_mapping['unique_id'], _id = last_record_id, timing = last_record_id.generation_time)
                    processed_collection = {}
                    break
                if(killer.kill_now):
                    self.save_job_working_data()
                    msg = "Migration stopped for *{0}* from database *{1}* ({2}) to *{3}*\n".format(self.curr_mapping['collection_name'], self.db['source']['db_name'], self.db['source']['source_type'], self.db['destination']['destination_type'])
                    msg += "Reason: Caught sigterm :warning:\n"
                    msg += "Insertions: {0}\nUpdations: {1}".format("{:,}".format(self.n_insertions), "{:,}".format(self.n_updations))
                    slack_token = settings['slack_notif']['slack_token']
                    channel = self.curr_mapping['slack_channel'] if 'slack_channel' in self.curr_mapping and self.curr_mapping['slack_channel'] else settings['slack_notif']['channel']
                    if('notify' in settings.keys() and settings['notify']):
                        send_message(msg = msg, channel = channel, slack_token = slack_token)
                        self.inform('Notification sent.')
                    raise Sigterm("Ending gracefully.")
            time.sleep(self.time_delay)
        self.inform(message = "Logging operation complete.", save=True)


    def syncing_process(self) -> None:
        '''
            Function to handle all syncing process. First we find all records to be inserted and make the changes at destination
            Then we find all records to be updated and overwrite those records sequentially in destination.
        '''
        ## FIRST WE NEED TO UPDATE THOSE RECORDS WHICH WERE INSERTED IN LAST RUN, BUT AN ERROR WAS ENCOUNTERED, AND THOSE WERE UPDATED AT SOURCE LATER ON
        recovery_data = get_recovery_data(self.curr_mapping['unique_id'])
        if(recovery_data):
            self.inform("Trying to make the system recover by updating the records inserted during the previously failed job(s).")
            start = 0
            updated_in_destination = True
            processed_collection = {}
            processed_collection_u = {}
            end = 0
            while(True):
                end = start + self.batch_size
                if('improper_bookmarks' not in self.curr_mapping.keys()):
                    self.curr_mapping['improper_bookmarks'] = True
                processed_collection_u = {}
                processed_collection_u = self.updating_recovered_data(start=start, end=end, improper_bookmarks=self.curr_mapping['improper_bookmarks'], last_recovered_record = recovery_data['record_id'], recovered_data_timing = recovery_data['timing'])
                '''
                    self.updating_recovered_data() returns either None or Dict[str, Any]
                    None is returned when no more records need to be checked for updations and all updations have been identified.
                    Dict[str, Any] is returned in case either updations are found, or if not found then there is scope to find updations in next batch
                '''
                if(processed_collection_u is not None):
                    if(updated_in_destination):
                        processed_collection['df_update'] = processed_collection_u['df_update']
                    else:
                        processed_collection['df_update'] = typecast_df_to_schema(processed_collection['df_update'].append([processed_collection_u['df_update']]), self.curr_mapping['fields'])
                    self.inform(message="Found " + str(processed_collection['df_update'].shape[0]) + " updations upto now.")
                else:
                    if(processed_collection):
                        self.save_data(processed_collection=processed_collection)
                        updated_in_destination = True
                        processed_collection = {}
                    break
                if('df_update' in processed_collection.keys()):
                    if(processed_collection['df_update'].shape[0] >= self.batch_size):
                        self.save_data(processed_collection=processed_collection)
                        updated_in_destination = True
                        processed_collection = {}
                    else:
                        updated_in_destination = False
                        ## Still not saved the updates, will update together a large number of records...will save time
                time.sleep(self.time_delay)
                start += self.batch_size
            self.inform(message="Updation completed for recovered data.")
            

        ## NOW, SYSTEM IS RECOVERED, LET'S FOCUS ON INSERTING NEW DATA
        self.inform(message="Starting to migrate newly inserted records.")
        processed_collection = {}
        while(True):
            processed_collection = self.adding_new_data(mode='syncing')
            if(not processed_collection):
                self.inform(message="Insertions complete.")
                break
            else:
                killer = NormalKiller()
                if(self.curr_mapping['cron'] == 'self-managed'):
                    killer = GracefulKiller()
                while not killer.kill_now:
                    self.save_data(processed_collection=processed_collection)
                    if(processed_collection['df_insert'].shape[0]):
                        last_record_id = ObjectId(processed_collection['df_insert']['_id'].iloc[-1])
                        set_last_migrated_record(job_id = self.curr_mapping['unique_id'], _id = last_record_id, timing = last_record_id.generation_time)
                        save_recovery_data(job_id = self.curr_mapping['unique_id'], _id = last_record_id, timing = self.curr_run_cron_job)
                    processed_collection = {}
                    break
                if(killer.kill_now):
                    self.save_job_working_data()
                    msg = "Migration stopped for *{0}* from database *{1}* ({2}) to *{3}*\n".format(self.curr_mapping['collection_name'], self.db['source']['db_name'], self.db['source']['source_type'], self.db['destination']['destination_type'])
                    msg += "Reason: Caught sigterm :warning:\n"
                    msg += "Insertions: {0}\nUpdations: {1}".format("{:,}".format(self.n_insertions), "{:,}".format(self.n_updations))
                    slack_token = settings['slack_notif']['slack_token']
                    channel = self.curr_mapping['slack_channel'] if 'slack_channel' in self.curr_mapping and self.curr_mapping['slack_channel'] else settings['slack_notif']['channel']
                    if('notify' in settings.keys() and settings['notify']):
                        send_message(msg = msg, channel = channel, slack_token = slack_token)
                        self.inform('Notification sent.')
                    raise Sigterm("Ending gracefully.")
            time.sleep(self.time_delay)
        self.inform(message = "Insertions completed, starting to update records", save = True)

        ## NOW, DO ALL REQUIRED UPDATIONS
        self.inform(message="Starting to migrate updations in records.")
        start = 0
        updated_in_destination = True
        processed_collection = {}
        processed_collection_u = {}
        end = 0
        while(True):
            end = start + self.batch_size
            if('improper_bookmarks' not in self.curr_mapping.keys()):
                self.curr_mapping['improper_bookmarks'] = True
            processed_collection_u = {}
            processed_collection_u = self.updating_data(start=start, end=end, improper_bookmarks=self.curr_mapping['improper_bookmarks'])
            '''
                self.updating_data() returns either None or Dict[str, Any]
                None is returned when no more records need to be checked for updations and all updations have been identified.
                Dict[str, Any] is returned in case either updations are found, or if not found then there is scope to find updations in next batch
            '''
            if(processed_collection_u is not None):
                if(updated_in_destination):
                    processed_collection['df_update'] = processed_collection_u['df_update']
                else:
                    processed_collection['df_update'] = typecast_df_to_schema(processed_collection['df_update'].append([processed_collection_u['df_update']]), self.curr_mapping['fields'])
                self.inform(message="Found " + str(processed_collection['df_update'].shape[0]) + " updations upto now.")
            else:
                if(processed_collection):
                    self.save_data(processed_collection=processed_collection)
                    updated_in_destination = True
                    processed_collection = {}
                break
            if('df_update' in processed_collection.keys()):
                if(processed_collection['df_update'].shape[0] >= self.batch_size):
                    self.save_data(processed_collection=processed_collection)
                    updated_in_destination = True
                    processed_collection = {}
                else:
                    updated_in_destination = False
                    ## Still not saved the updates, will update together a large number of records...will save time
            time.sleep(self.time_delay)
            start += self.batch_size
        self.inform(message="Updation completed.")
    
        ## NOW, CHECK FOR BUFFER UPDATION LAG
        if('buffer_updation_lag' in self.curr_mapping.keys() and self.curr_mapping['buffer_updation_lag']):
            buffer_days = self.curr_mapping['buffer_updation_lag']['days'] if 'days' in self.curr_mapping['buffer_updation_lag'].keys() and self.curr_mapping['buffer_updation_lag']['days'] else 0
            buffer_hours = self.curr_mapping['buffer_updation_lag']['hours'] if 'hours' in self.curr_mapping['buffer_updation_lag'].keys() and self.curr_mapping['buffer_updation_lag']['hours'] else 0
            buffer_minutes = self.curr_mapping['buffer_updation_lag']['minutes'] if 'minutes' in self.curr_mapping['buffer_updation_lag'].keys() and self.curr_mapping['buffer_updation_lag']['minutes'] else 0

            self.inform("Starting updation-check for newly inserted records which were updated in the {0} days, {1} hours and {2} minutes before the job started.".format(buffer_days, buffer_hours, buffer_minutes))
            start = 0
            updated_in_destination = True
            processed_collection = {}
            processed_collection_u = {}
            end = 0
            while(True):
                end = start + self.batch_size
                if('improper_bookmarks' not in self.curr_mapping.keys()):
                    self.curr_mapping['improper_bookmarks'] = True
                processed_collection_u = {}
                processed_collection_u = self.buffer_updating_data(start=start, end=end, improper_bookmarks=self.curr_mapping['improper_bookmarks'], buffer_days=buffer_days, buffer_hours=buffer_hours, buffer_minutes=buffer_minutes)
                '''
                    self.updating_data() returns either None or Dict[str, Any]
                    None is returned when no more records need to be checked for updations and all updations have been identified.
                    Dict[str, Any] is returned in case either updations are found, or if not found then there is scope to find updations in next batch
                '''
                if(processed_collection_u is not None):
                    if(updated_in_destination):
                        processed_collection['df_update'] = processed_collection_u['df_update']
                    else:
                        processed_collection['df_update'] = typecast_df_to_schema(processed_collection['df_update'].append([processed_collection_u['df_update']]), self.curr_mapping['fields'])
                    self.inform(message="Found " + str(processed_collection['df_update'].shape[0]) + " updations upto now.")
                else:
                    if(processed_collection):
                        self.save_data(processed_collection=processed_collection)
                        updated_in_destination = True
                        processed_collection = {}
                    break
                if('df_update' in processed_collection.keys()):
                    if(processed_collection['df_update'].shape[0] >= self.batch_size):
                        self.save_data(processed_collection=processed_collection)
                        updated_in_destination = True
                        processed_collection = {}
                    else:
                        updated_in_destination = False
                        ## Still not saved the updates, will update together a large number of records...will save time
                time.sleep(self.time_delay)
                start += self.batch_size
            self.inform(message="Buffer_updation completed.")

        self.inform(message="Syncing operation complete (Both - Insertion and Updation).", save=True)


    def save_data(self, processed_collection: Dict[str, Any] = None) -> None:
        '''
            processed_collection is a dictionary type object with fields: name (of the collection), df_insert (dataframe of records to be inserted), df_update (dataframe of records to be deleted) and dtypes (athena datatypes one to one mapping for every column in dataframe)
            This function saves the processed data into destination
        '''
        if(not processed_collection):
            return
        else:
            ## If there is data in processed collection and some field is missing, we handle it by setting the field by some value (mostly empty)
            if('name' not in processed_collection.keys()):
                processed_collection['name'] = self.curr_mapping['collection_name']
            if('df_insert' not in processed_collection.keys()):
                processed_collection['df_insert'] = pd.DataFrame({})
            if('df_update' not in processed_collection.keys()):
                processed_collection['df_update'] = pd.DataFrame({})
            if('dtypes' not in processed_collection.keys()):
                processed_collection['dtypes'] = get_athena_dtypes(self.curr_mapping['fields'])
            if('lob_fields_length' in self.curr_mapping.keys() and self.curr_mapping['lob_fields_length']):
                processed_collection['lob_fields_length'] = self.curr_mapping['lob_fields_length']
            if('col_rename' in self.curr_mapping.keys() and self.curr_mapping['col_rename']):
                processed_collection['col_rename'] = self.curr_mapping['col_rename']
            primary_keys = []
            if(self.curr_mapping['mode'] != 'dumping'):
                ## If mode is dumping, then there can't be any primary key (because multiple copies are present for same record). Otherwise, set _id as primary_key
                primary_keys = ['_id']
            self.n_insertions += processed_collection['df_insert'].shape[0]
            self.n_updations += processed_collection['df_update'].shape[0]
            if(processed_collection['df_insert'].shape[0]):
                self.curr_megabytes_processed += processed_collection['df_insert'].memory_usage(index=True).sum()
            if(processed_collection['df_update'].shape[0]):
                self.curr_megabytes_processed += processed_collection['df_update'].memory_usage(index=True).sum()
            self.saver.save(processed_data = processed_collection, primary_keys = primary_keys)


    def process(self) -> Tuple[int]:
        '''
            This function handles the entire flow of preprocessing, processing, saving and postprocessing data.
        '''
        if(self.curr_mapping['mode'] == 'mirroring' and self.db['destination']['destination_type'] == 's3'):
            raise IncorrectMapping("Mirroring mode not supported for destination S3")
        
        self.get_connectivity()
        self.inform(message="Connected to database and collection.", save=True)
        self.preprocess()
        self.inform(message="Collection pre-processed.", save=True)

        '''
            Mode = 'syncing', 'logging', 'dumping' or 'mirroring'
            When we are dumping data, snapshots of the datastore are captured at regular intervals. We maintain multiple copies of the tables
            Logging is the mode where the data is only being added to the source table, and we assume no updations are ever performed. Only new records are migrated.
            Syncing: Where all new records is migrated, and all updations are also mapped to destination. If a record is deleted at source, it's NOT deleted at destination
            Mirroring: A mirror image of source db is maintained at destination
        '''
        try:
            if(self.curr_mapping['mode'] == 'dumping'):
                self.dumping_process()
            elif(self.curr_mapping['mode'] == 'logging'):
                self.logging_process()
            elif(self.curr_mapping['mode'] == 'syncing'):
                self.syncing_process()
            elif(self.curr_mapping['mode'] == 'mirroring'):
                self.dumping_process()
            else:
                raise IncorrectMapping("Please specify a mode of operation.")
        except KeyboardInterrupt:
            self.save_job_working_data()
            raise
        except Sigterm as e:
            raise
        except Exception as e:
            self.save_job_working_data()
            raise
        else:
            self.save_job_working_data()
        self.postprocess()
        self.inform(message="Post processing completed.", save=True)

        self.saver.close()
        self.inform(message="Hope to see you again :')")

        return (self.n_insertions, self.n_updations)