from pymongo import MongoClient
from helper.util import convert_list_to_string, convert_to_type
from helper.logger import logger
import certifi
from dst.s3 import save_to_s3
import pandas as pd
import datetime
from config.migration_mapping import encryption_store
import json
import hashlib

def check_if_present():
    pass

def dataframe_from_collection(current_collection, collection_unique_id, collection_format={}, curr_collection_schema={}):
    '''
        Converts the unstructed database documents to structured pandas dataframe
    '''
    docu = []
    docu_update = []
    count = 0
    total_len = current_collection.count_documents({})
    logger.info("Total " + str(total_len) + " documents present in collection.")
    client_encr = MongoClient(encryption_store['url'], tlsCAFile=certifi.where())
    db_encr = client_encr[encryption_store['db_name']]
    collection_encr = db_encr[encryption_store['collection_name']]
    collection_documents = current_collection.find()
    if(curr_collection_schema['bookmark']):
        if('last_run_cron_job' not in curr_collection_schema.keys()):
            curr_collection_schema['last_run_cron_job'] = datetime.datetime(1602, 8, 20)
        collection_documents = current_collection.find({curr_collection_schema['bookmark']: {"$gt": curr_collection_schema['last_run_cron_job']}})
        curr_collection_schema['last_run_cron_job'] = datetime.datetime.now()
    logger.info("Fetched all candidate documents from collection.")
    for document in collection_documents:
        document_to_be_encrypted = document.copy()
        document_to_be_encrypted['_id'] = str(document_to_be_encrypted['_id'])
        encr = {
            'collection': collection_unique_id,
            'map_id': document['_id'],
            'document_sha': hashlib.sha1(json.dumps(document_to_be_encrypted, default=str).encode()).hexdigest()
        }
        updation = False
        if(not curr_collection_schema['bookmark']):
            previous_records = collection_encr.find_one({'collection': collection_unique_id, 'map_id': document['_id']})
            if(previous_records):
                if(previous_records['document_sha'] == encr['document_sha']):
                    continue     # No updation in this record
                else:
                    updation = True     # This record has been updated
                    collection_encr.delete_one({'collection': collection_unique_id, 'map_id': document['_id']})
        
        collection_encr.insert_one(encr)
        document['parquet_format_date_year'] = (document['_id'].generation_time - datetime.timedelta(hours=5, minutes=30)).year
        document['parquet_format_date_month'] = (document['_id'].generation_time - datetime.timedelta(hours=5, minutes=30)).month
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
        if(count % 5 == 0):
            logger.info(str(count)+ " documents fetched ... " + str(int(count*100/total_len)) + " %")
    
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
