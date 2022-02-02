from pymongo import MongoClient
from helper.util import convert_list_to_string, convert_to_type, convert_to_datetime
from helper.logger import logger
import certifi
from dst.s3 import save_to_s3
import pandas as pd
import datetime
from config.migration_mapping import encryption_store
import json
import hashlib
import pytz

utc_tz = pytz.UTC

def dataframe_from_collection(current_collection, collection_unique_id, collection_format={}, curr_collection_schema={}):
    '''
        Converts the unstructed database documents to structured pandas dataframe
    '''
    docu = []
    docu_update = []
    count = 0
    total_len = current_collection.count_documents({})
    logger.info("Total " + str(total_len) + " documents present in collection.")

    ## Fetching encryption database
    ## Encryption database is used to store hashes of records in case bookmark is absent
    client_encr = MongoClient(encryption_store['url'], tlsCAFile=certifi.where())
    db_encr = client_encr[encryption_store['db_name']]
    collection_encr = db_encr[encryption_store['collection_name']]
 
    if('last_run_cron_job' not in curr_collection_schema.keys()):
        curr_collection_schema['last_run_cron_job'] = utc_tz.localize(datetime.datetime(1602, 8, 20, 0, 0, 0, 0))
    
    for document in current_collection.find():
        document['parquet_format_date_year'] = (document['_id'].generation_time - datetime.timedelta(hours=5, minutes=30)).year
        document['parquet_format_date_month'] = (document['_id'].generation_time - datetime.timedelta(hours=5, minutes=30)).month
        
        updation = False
        insertion_time = document['_id'].generation_time
        if(insertion_time < curr_collection_schema['last_run_cron_job']):
            # Document of this _id would already be transferred
            # Now we need to check if any updation has been performed
            # If bookmark present, use it
            # If not, store a hash 
            if(curr_collection_schema['bookmark']):
                # In case there is bookmark present for comparison
                if('bookmark_format' not in curr_collection_schema.keys()):
                    if(convert_to_datetime(document[curr_collection_schema['bookmark']], curr_collection_schema['bookmark_format']) <= curr_collection_schema['last_run_cron_job']):
                        continue
                    else:
                        updation = True
                else:
                    if(document[curr_collection_schema['bookmark']] <= curr_collection_schema['last_run_cron_job']):
                        continue
                    else:
                        updation = True
            else:
                # In case bookmark is not available
                # We will store a hash to check for updation
                document['_id'] = str(document['_id'])
                encr = {
                    'collection': collection_unique_id,
                    'map_id': document['_id'],
                    'document_sha': hashlib.sha256(json.dumps(document, default=str).encode()).hexdigest()
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
            if(curr_collection_schema['bookmark']):
                pass
            else:
                # We need to store hash if bookmark is absent
                document['_id'] = str(document['_id'])
                encr = {
                    'collection': collection_unique_id,
                    'map_id': document['_id'],
                    'document_sha': hashlib.sha256(json.dumps(document, default=str).encode()).hexdigest()
                }
                collection_encr.insert_one(encr)

        for key, value in document.items():
            if(key == '_id'):
                document[key] = str(document[key])
            elif(key in collection_format.keys()):
                document[key] = convert_to_type(document[key], collection_format[key])
            elif(isinstance(document[key], int) or isinstance(document[key], float) or isinstance(document[key], complex)):
                document[key] = str(document[key])
            elif(isinstance(document[key], list)):
                document[key] = convert_list_to_string(document[key])
            elif(isinstance(document[key], bool)):
                document[key] = str(document[key])
        count += 1
        if(not updation):
            docu.append(document)
        else:
            docu_update.append(document)
        if(count % 10000 == 0):
            logger.info(str(count)+ " documents fetched ... " + str(int(count*100/total_len)) + " %")
    
    curr_collection_schema['last_run_cron_job'] = datetime.datetime.now()
    if(total_len > 0):
        logger.info(str(count)+ " documents fetched ... " + str(int(count*100/total_len)) + " %")
    ret_df = pd.DataFrame(docu)
    ret_df_update = pd.DataFrame(docu_update)
    logger.info("Converted " + str(count) + " documents to dataframe objects")
    logger.info("Number of new records to be inserted " + str(ret_df.shape[0]))        
    logger.info("Number of records to be updated " + str(ret_df_update.shape[0]))        
    return ret_df, ret_df_update

def get_data_from_source(db):
    client = MongoClient(db['url'], tlsCAFile=certifi.where())
    if('collections' not in db.keys()):
        db['collections'] = []
    database_ = client[db['db_name']]
    for curr_collection in db['collections']:
        curr_collection['collection_unique_id'] = db['db_name'] + '-|-' + curr_collection['collection_name']
    return database_

def process_data_from_source(database_, collection_name = []):
    df_list = []
    for curr_collection in collection_name:
        if('fields' not in curr_collection.keys()):
            curr_collection['fields'] = {}
        df_collection, df_collection_update = dataframe_from_collection(database_[curr_collection['collection_name']], collection_unique_id=curr_collection['collection_unique_id'], collection_format=curr_collection['fields'], curr_collection_schema = curr_collection)
        df_list.append({'collection_name': curr_collection['collection_name'], 'df_collection': df_collection, 'df_collection_update': df_collection_update})
    return df_list
    
def save_data_to_destination(db, df_list):
    if(db['destination_type'] == 's3'):
        save_to_s3(df_list, db)

def process_mongodb(db):
    logger.info('Time to connect to db ' + db['db_name'])
    database_ = get_data_from_source(db)
    logger.info('Fetched data from database ' + str(db['db_name']) + ".")
    df_list = process_data_from_source(database_=database_, collection_name=db['collections'])
    logger.info('Processed data from database ' + str(db['db_name']) + ". Attempting to save.")
    save_data_to_destination(db=db, df_list=df_list)
    logger.info('Saved data from database ' + str(db['db_name']) + ": Success")
