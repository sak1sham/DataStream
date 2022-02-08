from pymongo import MongoClient
from helper.util import convert_list_to_string, convert_to_type, convert_to_datetime, convert_json_to_string
from helper.logger import log_writer
import certifi
from dst.s3 import save_to_s3
import pandas as pd
import datetime
import json
import hashlib
import pytz
from db.encr_db import get_data_from_encr_db, get_last_run_cron_job

IST_tz = pytz.timezone('Asia/Kolkata')

def dataframe_from_collection(mongodb_collection, collection_mapping={}):
    '''
        Converts the unstructed database documents to structured pandas dataframe
        INPUT:
            mongodb_collection: Pymongo collection
            collection_mapping: Full collection mapping as provided in migration_mapping.py
        OUTPUT:
            df_insert: New Records to insert at destination (pandas.DataFrame)
            df_update: Updations to be done in existing records at destination (pandas.DataFrame)
    '''
    collection_unique_id=collection_mapping['collection_unique_id']
    collection_fields=collection_mapping['fields']
    docu_insert = []
    docu_update = []
    count = 0
    total_len = mongodb_collection.count_documents({})
    log_writer("Total " + str(total_len) + " documents present in collection.")
    one_percent = total_len//100

    ## Fetching encryption database
    ## Encryption database is used to store hashes of records in case bookmark is absent
    collection_encr = get_data_from_encr_db()
    if(collection_encr is None):
        return None, None

    last_run_cron_job = get_last_run_cron_job(collection_encr, collection_mapping['collection_unique_id'])

    if('to_partition' in collection_mapping.keys() and collection_mapping['to_partition']):
        if('partition_col' not in collection_mapping.keys() or not collection_mapping['partition_col']):
            log_writer("Partition_col not specified in " + collection_mapping['collection_unique_id'] + "\'s mapping. Making partition using _id.")
            collection_mapping['partition_col'] = ['_id']
            collection_mapping['partition_col_format'] = ['datetime']
        if(isinstance(collection_mapping['partition_col'], str)):
            collection_mapping['partition_col'] = [collection_mapping['partition_col']]
        if('partition_col_format' not in collection_mapping.keys()):
            collection_mapping['partition_col_format'] = ['str']
        if(isinstance(collection_mapping['partition_col_format'], str)):
            collection_mapping['partition_col_format'] = [collection_mapping['partition_col_format']]
        while(len(collection_mapping['partition_col']) > len(collection_mapping['partition_col_format'])):
            collection_mapping['partition_col_format'] = collection_mapping['partition_col_format'].append('str')
        # Now, there is a 1-1 mapping of partition_col and partition_col_format. Now, we need to partition.

    collection_mapping['partition_for_parquet'] = []
    if('to_partition' in collection_mapping.keys() and collection_mapping['to_partition']):
        for i in range(len(collection_mapping['partition_col'])):
            col = collection_mapping['partition_col'][i]
            col_form = collection_mapping['partition_col_format'][i]
            parq_col = "parquet_format_" + col
            if(col == 'migration_snapshot_date'):
                col_form ='datetime'
                collection_fields[col] = 'datetime'
                collection_mapping['partition_col_format'][i] = 'datetime'
                collection_mapping['partition_for_parquet'].extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                collection_fields[parq_col + "_year"] = 'int'
                collection_fields[parq_col + "_month"] = 'int'
                collection_fields[parq_col + "_day"] = 'int'
            elif(col == '_id'):
                collection_mapping['partition_for_parquet'].extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                collection_fields[parq_col + "_year"] = 'int'
                collection_fields[parq_col + "_month"] = 'int'
                collection_fields[parq_col + "_day"] = 'int'
            elif(col_form == 'str'):
                collection_mapping['partition_for_parquet'].extend([parq_col])
            elif(col_form == 'int'):
                collection_mapping['partition_for_parquet'].extend([parq_col])
                collection_fields[parq_col] = 'int'
            elif(col_form == 'datetime'):
                collection_mapping['partition_for_parquet'].extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                collection_fields[parq_col + "_year"] = 'int'
                collection_fields[parq_col + "_month"] = 'int'
                collection_fields[parq_col + "_day"] = 'int'
            else:
                collection_mapping['partition_for_parquet'].extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                collection_fields[parq_col + "_year"] = 'int'
                collection_fields[parq_col + "_month"] = 'int'
                collection_fields[parq_col + "_day"] = 'int'

    all_documents = mongodb_collection.find()

    for document in all_documents:
        insertion_time = IST_tz.normalize(document['_id'].generation_time.astimezone(IST_tz))
        if('is_dump' in collection_mapping.keys() and collection_mapping['is_dump']):
            document['migration_snapshot_date'] = datetime.datetime.utcnow().replace(tzinfo = IST_tz)
        if('to_partition' in collection_mapping.keys() and collection_mapping['to_partition'] and 'partition_col' in collection_mapping.keys()):        
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
                    document[parq_col] = int(document[col])
                elif(col_form == 'datetime'):
                    document[parq_col + "_year"] = document[col].year
                    document[parq_col + "_month"] = document[col].month
                    document[parq_col + "_day"] = document[col].day
                else:
                    temp = convert_to_datetime(document[col], col_form)
                    document[parq_col + "_year"] = temp.year
                    document[parq_col + "_month"] = temp.month
                    document[parq_col + "_day"] = temp.day

        updation = False
        if('is_dump' not in collection_mapping.keys() or not collection_mapping['is_dump']):
            if(insertion_time < last_run_cron_job):
                # Document of this _id would already be present at destination, we need to check for updation
                if('bookmark' in collection_mapping.keys() and collection_mapping['bookmark']):
                    # Use bookmark for comparison of updation time
                    if('bookmark_format' not in collection_mapping.keys()):
                        if(document[collection_mapping['bookmark']] <= last_run_cron_job):
                            # No updation has been performed since last cron job
                            continue
                        else:
                            # The document has changed. Need to update destination
                            updation = True
                    else:
                        if(convert_to_datetime(document[collection_mapping['bookmark']], collection_mapping['bookmark_format']) <= last_run_cron_job):
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
                # If bookmark is present, and document has not been seen before, we will insert the document with updation = False

        for key, _ in document.items():
            if(key == '_id'):
                document[key] = str(document[key])
            elif(key in collection_fields.keys()):
                document[key] = convert_to_type(document[key], collection_fields[key])
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
                    log_writer("Unidentified datatype at id:" + str(document['_id']) + " in " + str(collection_unique_id) + ". Saving empty string.")
                    document[key] = ""
        count += 1
        if(not updation):
            docu_insert.append(document)
        else:
            docu_update.append(document)
        if(one_percent > 0 and count % one_percent == 0):
            log_writer(str(count)+ " documents fetched ... " + str(int(count*100/total_len)) + " %")
    
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
    
def save_data_to_destination(db, processed_collection, partition):
    if(db['destination']['destination_type'] == 's3'):
        save_to_s3(processed_collection, db_source=db['source'], db_destination=db['destination'], c_partition=partition)

def process_mongo_collection(db, collection):
    log_writer('Migrating ' + collection['collection_unique_id'])
    db_collection = get_data_from_source(db, collection['collection_name'])
    if(db_collection is not None):
        log_writer('Fetched data for ' + collection['collection_unique_id'])
        processed_collection = process_data_from_source(db_collection=db_collection, collection=collection)
        if(processed_collection is not None):
            log_writer('Processed data for ' + collection['collection_unique_id'])
            try:
                save_data_to_destination(db=db, processed_collection=processed_collection, partition=collection['partition_for_parquet'])
                log_writer('Successfully saved data for ' + collection['collection_unique_id'])
            except:
                log_writer('Caught some exception while saving data from ' + collection['collection_unique_id'])
    log_writer("Migration for " + collection['collection_unique_id'] + " ended.")
