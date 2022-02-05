from pymongo import MongoClient
from helper.util import convert_list_to_string, convert_to_type, convert_to_datetime
from helper.logger import log_writer
import certifi
from dst.s3 import save_to_s3
import pandas as pd
import datetime
import json
import hashlib
import pytz
from encr_db import get_data_from_encr_db

IST_tz = pytz.timezone('Asia/Kolkata')

def dataframe_from_collection(mongodb_collection, collection_mapping={}):
    '''
        Converts the unstructed database documents to structured pandas dataframe
    '''
    collection_unique_id=collection_mapping['collection_unique_id']
    collection_fields=collection_mapping['fields']
    docu_insert = []
    docu_update = []
    count = 0
    total_len = mongodb_collection.count_documents({})
    log_writer("Total " + str(total_len) + " documents present in collection.")

    ## Fetching encryption database
    ## Encryption database is used to store hashes of records in case bookmark is absent
    
    collection_encr = get_data_from_encr_db()
    if(collection_encr is None):
        return None, None

    if('last_run_cron_job' not in collection_mapping.keys()):
        collection_mapping['last_run_cron_job'] = IST_tz.localize(datetime.datetime(1602, 8, 20, 0, 0, 0, 0))
    
    for document in mongodb_collection.find():
        insertion_time = IST_tz.normalize(document['_id'].generation_time.astimezone(IST_tz))
        if('to_partition' in collection_mapping.keys() and collection_mapping['to_partition']):
            if('partition_col' not in collection_mapping.keys() or not collection_mapping['partition_col']):
                document['parquet_format_date_year'] = insertion_time.year
                document['parquet_format_date_month'] = insertion_time.month
            else:
                if('partition_col_format' not in collection_mapping.keys() or not collection_mapping['partition_col_format']):
                    document['parquet_format_date_year'] = document[collection_mapping['partition_col']].year
                    document['parquet_format_date_month'] = document[collection_mapping['partition_col']].month
                else:
                    document['parquet_format_date_year'] = convert_to_datetime(document[collection_mapping['partition_col']], collection_mapping['partition_col_format']).year
                    document['parquet_format_date_month'] = convert_to_datetime(document[collection_mapping['partition_col']], collection_mapping['partition_col_format']).month
            collection_fields['parquet_format_date_year'] = 'integer'
            collection_fields['parquet_format_date_month'] = 'integer'

        updation = False
        if(insertion_time < collection_mapping['last_run_cron_job']):
            # Document of this _id would already be present at destination, we need to check for updation
            if(collection_mapping['bookmark']):
                # Use bookmark for comparison of updation time
                if('bookmark_format' not in collection_mapping.keys()):
                    if(document[collection_mapping['bookmark']] <= collection_mapping['last_run_cron_job']):
                        # No updation has been performed since last cron job
                        continue
                    else:
                        # The document has changed. Need to update destination
                        updation = True
                else:
                    if(convert_to_datetime(document[collection_mapping['bookmark']], collection_mapping['bookmark_format']) <= collection_mapping['last_run_cron_job']):
                        # No updation has been performed since last cron job
                        continue
                    else:
                        # The document has changed. Need to update destination
                        updation = True
            else:
                # No bookmark => We will store a hash to check for updation
                document['_id'] = str(document['_id'])
                encr = {
                    'collection': collection_unique_id,
                    'map_id': document['_id'],
                    'document_sha': hashlib.sha256(json.dumps(document, default=str, sort_keys=True).encode()).hexdigest()
                }
                previous_records = collection_encr.find_one({'collection': collection_unique_id, 'map_id': document['_id']})
                if(previous_records):
                    if(previous_records['document_sha'] == encr['document_sha']):
                        continue     # No updation in this record
                    else:
                        updation = True     # This record has been updated
                        collection_encr.delete_one({'collection': collection_unique_id, 'map_id': document['_id']})
                        collection_encr.insert_one(encr)
                else:
                    collection_encr.insert_one(encr)
        else:
            ## Document has not been seen before
            if(not collection_mapping['bookmark']):
                # We need to store hash if bookmark is not present
                document['_id'] = str(document['_id'])
                encr = {
                    'collection': collection_unique_id,
                    'map_id': document['_id'],
                    'document_sha': hashlib.sha256(json.dumps(document, default=str, sort_keys=True).encode()).hexdigest()
                }
                collection_encr.insert_one(encr)
            # If bookmark is present, and document has not been seen before, we will go with flow and updation = False

        for key, _ in document.items():
            if(key == '_id'):
                document[key] = str(document[key])
            elif(key in collection_fields.keys()):
                document[key] = convert_to_type(document[key], collection_fields[key])
            elif(isinstance(document[key], int) or isinstance(document[key], float) or isinstance(document[key], complex)):
                document[key] = str(document[key])
            elif(isinstance(document[key], list)):
                document[key] = convert_list_to_string(document[key])
            elif(isinstance(document[key], bool)):
                document[key] = str(document[key])
        count += 1
        if(not updation):
            docu_insert.append(document)
        else:
            docu_update.append(document)
        if(count % 10000 == 0):
            log_writer(str(count)+ " documents fetched ... " + str(int(count*100/total_len)) + " %")
    
    collection_mapping['last_run_cron_job'] = datetime.datetime.utcnow().replace(tzinfo = IST_tz)
    log_writer(str(count) + " documents fetched.")
    ret_df_insert = pd.DataFrame(docu_insert)
    ret_df_update = pd.DataFrame(docu_update)
    log_writer("Converted " + str(count) + " collections to dataframe.")
    log_writer("Insertions: " + str(ret_df_insert.shape[0]))        
    log_writer("Updations: " + str(ret_df_update.shape[0]))        
    return ret_df_insert, ret_df_update

def get_data_from_source(db, collection_name):
    try:
        client = MongoClient(db['source']['url'], tlsCAFile=certifi.where())
        database_ = client[db['source']['db_name']]
        target_collection = database_[collection_name]
        return target_collection
    except:
        log_writer("Unable to connect to mongo:" + db['source']['db_name'] + ":" + collection_name)
        return None

def process_data_from_source(db_collection, collection):
    try:
        if('fields' not in collection.keys()):
            collection['fields'] = {}
        df_insert, df_update = dataframe_from_collection(mongodb_collection = db_collection, collection_mapping = collection)
        if(df_insert is not None and df_update is not None):
            return {'name': collection['collection_name'], 'df_insert': df_insert, 'df_update': df_update}
        else:
            return None
    except:
        log_writer("Caught some exception while processing " + collection['collection_unique_id'])
        return None
    
def save_data_to_destination(db, processed_collection):
    if(db['destination']['destination_type'] == 's3'):
        save_to_s3(processed_collection, db_source=db['source'], db_destination=db['destination'])

def process_mongo_collection(db, collection):
    log_writer('Migrating ' + collection['collection_unique_id'])
    db_collection = get_data_from_source(db, collection['collection_name'])
    if(db_collection is not None):
        log_writer('Fetched data for ' + collection['collection_unique_id'])
        processed_collection = process_data_from_source(db_collection=db_collection, collection=collection)
        if(processed_collection is not None):
            log_writer('Processed data for ' + collection['collection_unique_id'])
            try:
                save_data_to_destination(db=db, processed_collection=processed_collection)
                log_writer('Successfully saved data for ' + collection['collection_unique_id'])
            except:
                log_writer('Caught some exception while saving data from ' + collection['collection_unique_id'])
