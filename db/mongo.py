from pymongo import MongoClient
from helper.util import convert_list_to_string, convert_to_type, convert_to_datetime, convert_json_to_string

import logging
logging.getLogger().setLevel(logging.INFO)

import certifi
from dst.s3 import save_to_s3
import pandas as pd
import datetime
import json
import hashlib
import pytz
from db.encr_db import get_data_from_encr_db, get_last_run_cron_job

IST_tz = pytz.timezone('Asia/Kolkata')

def dataframe_from_collection(mongodb_collection, collection_mapping={}, start=0, end=0):
    '''
        Converts the unstructed database documents to structured pandas dataframe
        INPUT:
            mongodb_collection: Pymongo collection
            collection_mapping: Full collection mapping as provided in migration_mapping.py
        OUTPUT:
            df_insert: New Records to insert at destination (pandas.DataFrame)
            df_update: Updations to be done in existing records at destination (pandas.DataFrame)
    '''
    docu_insert = []
    docu_update = []

    collection_encr = get_data_from_encr_db()
    last_run_cron_job = collection_mapping['last_run_cron_job']

    all_documents = mongodb_collection.find()[start:end]

    for document in all_documents:
        insertion_time = IST_tz.normalize(document['_id'].generation_time.astimezone(IST_tz))
        if('is_dump' in collection_mapping.keys() and collection_mapping['is_dump']):
            document['migration_snapshot_date'] = datetime.datetime.utcnow().replace(tzinfo = IST_tz)
        if('to_partition' in collection_mapping.keys() and collection_mapping['to_partition']):        
            for i in range(len(collection_mapping['partition_col'])):
                col = collection_mapping['partition_col'][i]
                col_form = collection_mapping['partition_col_format'][i]
                parq_col = "parquet_format_" + col
                if(col == '_id'):
                    document[parq_col + "_year"] = insertion_time.year
                    document[parq_col + "_month"] = insertion_time.month
                    document[parq_col + "_day"] = insertion_time.day
                elif(col_form == 'str'):
                    document[parq_col] = str(document[col])
                elif(col_form == 'int'):
                    document[parq_col] = float(document[col])
                elif(col_form == 'datetime'):
                    document[col] = convert_to_datetime(document[col])
                    document[parq_col + "_year"] = document[col].year
                    document[parq_col + "_month"] = document[col].month
                    document[parq_col + "_day"] = document[col].day
                else:
                    logging.error(collection_mapping['collection_unique_id'] + ": Parition_col_format wrongly specified.")
                    return None, None
                
        updation = False
        if('is_dump' not in collection_mapping.keys() or not collection_mapping['is_dump']):
            if(insertion_time < last_run_cron_job):
                if('bookmark' in collection_mapping.keys() and collection_mapping['bookmark']):
                    docu_bookmark_date = convert_to_datetime(document[collection_mapping['bookmark']])
                    if(docu_bookmark_date is not pd.Timestamp(None) and docu_bookmark_date <= last_run_cron_job):
                        continue
                    else:
                        updation = True
                else:
                    document['_id'] = str(document['_id'])
                    encr = {
                        'collection': collection_mapping['collection_unique_id'],
                        'map_id': document['_id'],
                        'document_sha': hashlib.sha256(json.dumps(document, default=str, sort_keys=True).encode()).hexdigest()
                    }
                    previous_records = collection_encr.find_one({'collection': collection_mapping['collection_unique_id'], 'map_id': document['_id']})
                    if(previous_records):
                        if(previous_records['document_sha'] == encr['document_sha']):
                            continue
                        else:
                            updation = True
                            collection_encr.delete_one({'collection': collection_mapping['collection_unique_id'], 'map_id': document['_id']})
                            collection_encr.insert_one(encr)
                    else:
                        collection_encr.insert_one(encr)
            else:
                if(not collection_mapping['bookmark']):
                    document['_id'] = str(document['_id'])
                    encr = {
                        'collection': collection_mapping['collection_unique_id'],
                        'map_id': document['_id'],
                        'document_sha': hashlib.sha256(json.dumps(document, default=str, sort_keys=True).encode()).hexdigest()
                    }
                    collection_encr.insert_one(encr)

        for key, _ in document.items():
            if(key == '_id'):
                document[key] = str(document[key])
            elif(key in collection_mapping['fields'].keys()):
                document[key] = convert_to_type(document[key], collection_mapping['fields'][key])
            elif(isinstance(document[key], list)):
                document[key] = convert_list_to_string(document[key])
            elif(isinstance(document[key], dict)):
                document[key] = convert_json_to_string(document[key])
            elif(isinstance(document[key], datetime.datetime)):
                document[key] = document[key].strftime("%Y-%m-%dT%H:%M:%S")
            else:
                try:
                    document[key] = str(document[key])
                except:
                    logging.warning(collection_mapping['collection_unique_id'] + ": Unidentified datatype at document _id:" + str(document['_id']) +". Saving NoneType.")
                    document[key] = None
        if(not updation):
            docu_insert.append(document)
        else:
            docu_update.append(document)
    
    ret_df_insert = pd.DataFrame(docu_insert)
    ret_df_update = pd.DataFrame(docu_update)
    return ret_df_insert, ret_df_update

def get_data_from_source(db, collection_name):
    try:
        client = MongoClient(db['source']['url'], tlsCAFile=certifi.where())
        database_ = client[db['source']['db_name']]
        target_collection = database_[collection_name]
        return target_collection
    except:
        logging.error("mongo:" + db['source']['db_name'] + ":" + collection_name + ": Unable to connect.")
        return None

def preprocessing(collection):
    try:
        if('fields' not in collection.keys()):
            collection['fields'] = {}
        
        collection_encr = get_data_from_encr_db()
        collection['last_run_cron_job'] = get_last_run_cron_job(collection_encr, collection['collection_unique_id'])

        if('to_partition' in collection.keys() and collection['to_partition']):
            # Assure that partition_col and parition_col_format are lists of same length. If not present, use "_id"
            if('partition_col' not in collection.keys() or not collection['partition_col']):
                logging.warning(collection['collection_unique_id'] + " Partition_col not specified. Making partition using _id.")
                collection['partition_col'] = ['_id']
                collection['partition_col_format'] = ['datetime']
            if(isinstance(collection['partition_col'], str)):
                collection['partition_col'] = [collection['partition_col']]
            if('partition_col_format' not in collection.keys()):
                collection['partition_col_format'] = ['str']
            if(isinstance(collection['partition_col_format'], str)):
                collection['partition_col_format'] = [collection['partition_col_format']]
            while(len(collection['partition_col']) > len(collection['partition_col_format'])):
                collection['partition_col_format'] = collection['partition_col_format'].append('str')
            # Now, there is a 1-1 mapping of partition_col and partition_col_format. Now, we need to partition.
        
        collection['partition_for_parquet'] = []
        if('to_partition' in collection.keys() and collection['to_partition']):
            for i in range(len(collection['partition_col'])):
                col = collection['partition_col'][i]
                col_form = collection['partition_col_format'][i]
                parq_col = "parquet_format_" + col
                if(col == 'migration_snapshot_date'):
                    collection['partition_col_format'][i] = 'datetime'
                    collection['fields'][col] = 'datetime'
                    collection['partition_for_parquet'].extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                    collection['fields'][parq_col + "_year"] = 'int'
                    collection['fields'][parq_col + "_month"] = 'int'
                    collection['fields'][parq_col + "_day"] = 'int'
                elif(col == '_id'):
                    collection['partition_for_parquet'].extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                    collection['fields'][parq_col + "_year"] = 'int'
                    collection['fields'][parq_col + "_month"] = 'int'
                    collection['fields'][parq_col + "_day"] = 'int'
                elif(col_form == 'str'):
                    collection['partition_for_parquet'].extend([parq_col])
                elif(col_form == 'int'):
                    collection['partition_for_parquet'].extend([parq_col])
                    collection['fields'][parq_col] = 'int'
                elif(col_form == 'datetime'):
                    collection['partition_for_parquet'].extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                    collection['fields'][parq_col + "_year"] = 'int'
                    collection['fields'][parq_col + "_month"] = 'int'
                    collection['fields'][parq_col + "_day"] = 'int'
                else:
                    logging.error(collection['collection_unique_id'] + ": Unrecognized partition_col_format " + str(col_form) + ". partition_col_format can be int, str or datetime")
                    return None
        return 1
    except:
        logging.error(collection['collection_unique_id'] + ": Caught some exception while preprocessing.")
        return None

def process_data_from_source(db_collection, collection, start, end):
    df_insert, df_update = dataframe_from_collection(mongodb_collection = db_collection, collection_mapping = collection, start=start, end=end)
    if(df_insert is not None and df_update is not None):
        return {'name': collection['collection_name'], 'df_insert': df_insert, 'df_update': df_update}
    else:
        return None
    
def save_data_to_destination(db, processed_collection, partition):
    if(db['destination']['destination_type'] == 's3'):
        save_to_s3(processed_collection, db_source=db['source'], db_destination=db['destination'], c_partition=partition)

def process_mongo_collection(db, collection):
    logging.info(collection['collection_unique_id'] + ': Migration started')
    batch_size = 10000
    db_collection = get_data_from_source(db, collection['collection_name'])
    if(db_collection is not None):
        logging.info(collection['collection_unique_id'] + ': Data fetched.')
        result = preprocessing(collection)
        if(result):
            logging.info(collection['collection_unique_id'] + ": Preprocessing done")
            start = 0
            while(start < db_collection.count_documents({})):    
                try:
                    processed_collection = process_data_from_source(db_collection=db_collection, collection=collection, start=start, end=min(start+batch_size, db_collection.count_documents({})))
                    save_data_to_destination(db=db, processed_collection=processed_collection, partition=collection['partition_for_parquet'])
                except:
                    logging.error(collection['collection_unique_id'] + ": Caught some error while migrating chunk.")
                    break
                start += batch_size

    logging.info(collection['collection_unique_id'] + ": Migration ended.\n")
