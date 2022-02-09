from helper.util import convert_list_to_string, convert_to_type, convert_to_datetime, convert_json_to_string
import logging
logging.getLogger().setLevel(logging.INFO)

from dst.s3 import save_to_s3
import pandas as pd
import datetime
import json
import hashlib
import pytz
from db.encr_db import get_data_from_encr_db, get_last_run_cron_job
import requests
IST_tz = pytz.timezone('Asia/Kolkata')

def dataframe_from_api(db_api, api_mapping={}, start=0, end=0):
    '''
        Converts the unstructed database documents to structured pandas dataframe
        INPUT:
            db_api: array of dict objects
            api_mapping: Full api mapping as provided in migration_mapping.py
        OUTPUT:
            df_insert: New Records to insert at destination (pandas.DataFrame)
            df_update: Updations to be done in existing records at destination (pandas.DataFrame)
    '''
    api_unique_id=api_mapping['api_unique_id']
    api_fields=api_mapping['fields']
    docu_insert = []
    docu_update = []

    ## Fetching encryption database
    ## Encryption database is used to store hashes of records in case bookmark is absent
    api_encr = get_data_from_encr_db()

    last_run_cron_job = api_mapping['last_run_cron_job']

    all_documents = db_api.find()

    for document in all_documents:
        if('is_dump' in api_mapping.keys() and api_mapping['is_dump']):
            document['migration_snapshot_date'] = datetime.datetime.utcnow().replace(tzinfo = IST_tz)
        if('to_partition' in api_mapping.keys() and api_mapping['to_partition'] and 'partition_col' in api_mapping.keys()):        
            for i in range(len(api_mapping['partition_col'])):
                col = api_mapping['partition_col'][i]
                col_form = api_mapping['partition_col_format'][i]
                parq_col = "parquet_format_" + col
                if(col_form == 'str'):
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

        if('is_dump' not in api_mapping.keys() or not api_mapping['is_dump']):
            # Document of this _id would already be present at destination, we need to check for updation
            if('bookmark' in api_mapping.keys() and api_mapping['bookmark']):
                if('bookmark_format' not in api_mapping.keys()):
                    if(document[api_mapping['bookmark']] <= last_run_cron_job):
                        continue
                else:
                    if(convert_to_datetime(document[api_mapping['bookmark']], api_mapping['bookmark_format']) <= last_run_cron_job):
                        continue

        for key, _ in document.items():
            if(key == '_id'):
                document[key] = str(document[key])
            elif(key in api_fields.keys()):
                document[key] = convert_to_type(document[key], api_fields[key])
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
                    logging.error(api_unique_id + ": unidentified datatype at id:" + str(document['_id']) +". Saving NoneType.")
                    document[key] = None
        
        updation = False
        if('is_dump' not in api_mapping.keys() or not api_mapping['is_dump']):    
            document_map = {}
            if('primary_keys' in api_mapping.keys() and api_mapping['primary_keys']):
                if(isinstance(api_mapping['primary_keys'], str)):
                    api_mapping['primary_keys'] = [api_mapping['primary_keys']]
                for k in api_mapping['primary_keys']:
                    document_map[k] = document[k]
            if(not document_map):
                document_map = document

            encr = {
                'api': api_unique_id,
                'map_id': json.dumps(document_map, default=str, sort_keys=True),
                'document_sha': hashlib.sha256(json.dumps(document, default=str, sort_keys=True).encode()).hexdigest()
            }
            previous_records = api_encr.find_one({'api': api_unique_id, 'map_id': document['_id']})
            if(previous_records):
                if(previous_records['document_sha'] == encr['document_sha']):
                    continue     # No updation in this record
                else:
                    updation = True     # This record has been updated
                    api_encr.delete_one({'api': api_unique_id, 'map_id': document['_id']})
            api_encr.insert_one(encr)
        
        if(not updation):
            docu_insert.append(document)
        else:
            docu_update.append(document)
    
    ret_df_insert = pd.DataFrame(docu_insert)
    ret_df_update = pd.DataFrame(docu_update)
    return ret_df_insert, ret_df_update

def get_data_from_source(db, api_name):
    try:
        response = requests.get(db['source']['url'], headers=db['source']['headers'])
        return response.text
    except:
        logging.error(api_name + ": Can't fetch data from API")
        return None

def preprocessing(api):
    try:
        if('fields' not in api.keys()):
            api['fields'] = {}
        
        api_encr = get_data_from_encr_db()
        api['last_run_cron_job'] = get_last_run_cron_job(api_encr, api['api_unique_id'])

        if('to_partition' in api.keys() and api['to_partition']):
            # Assure that partition_col and parition_col_format are lists of same length.
            if('partition_col' not in api.keys() or not api['partition_col']):
                logging.warning(api['api_unique_id'] + " Partition_col not specified. Continuing without partitioning.")
            elif(isinstance(api['partition_col'], str)):
                api['partition_col'] = [api['partition_col']]
            if('partition_col_format' not in api.keys()):
                api['partition_col_format'] = ['str']
            if(isinstance(api['partition_col_format'], str)):
                api['partition_col_format'] = [api['partition_col_format']]
            while('partition_col' in api.keys() and len(api['partition_col']) > len(api['partition_col_format'])):
                api['partition_col_format'] = api['partition_col_format'].append('str')
            # Now, there is a 1-1 mapping of partition_col and partition_col_format. Now, we need to partition.
        
        api['partition_for_parquet'] = []
        if('to_partition' in api.keys() and api['to_partition']):
            for i in range(len(api['partition_col'])):
                col = api['partition_col'][i]
                col_form = api['partition_col_format'][i]
                parq_col = "parquet_format_" + col
                if(col == 'migration_snapshot_date'):
                    col_form ='datetime'
                    api['fields'][col] = 'datetime'
                    api['partition_col_format'][i] = 'datetime'
                    api['partition_for_parquet'].extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                    api['fields'][parq_col + "_year"] = 'int'
                    api['fields'][parq_col + "_month"] = 'int'
                    api['fields'][parq_col + "_day"] = 'int'
                elif(col == '_id'):
                    api['partition_for_parquet'].extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                    api['fields'][parq_col + "_year"] = 'int'
                    api['fields'][parq_col + "_month"] = 'int'
                    api['fields'][parq_col + "_day"] = 'int'
                elif(col_form == 'str'):
                    api['partition_for_parquet'].extend([parq_col])
                elif(col_form == 'int'):
                    api['partition_for_parquet'].extend([parq_col])
                    api['fields'][parq_col] = 'int'
                elif(col_form == 'datetime'):
                    api['partition_for_parquet'].extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                    api['fields'][parq_col + "_year"] = 'int'
                    api['fields'][parq_col + "_month"] = 'int'
                    api['fields'][parq_col + "_day"] = 'int'
                else:
                    api['partition_for_parquet'].extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                    api['fields'][parq_col + "_year"] = 'int'
                    api['fields'][parq_col + "_month"] = 'int'
                    api['fields'][parq_col + "_day"] = 'int'
    except:
        logging.error(api['api_unique_id'] + ": caught some exception while preprocessing.")
        return None

def process_data_from_source(db_api, api):
    df_insert, df_update = dataframe_from_api(db_api = db_api, api_mapping = api)
    if(df_insert is not None and df_update is not None):
        return {'name': api['api_name'], 'df_insert': df_insert, 'df_update': df_update}
    else:
        return None
    
def save_data_to_destination(db, processed_api, partition):
    if(db['destination']['destination_type'] == 's3'):
        save_to_s3(processed_api, db_source=db['source'], db_destination=db['destination'], c_partition=partition)

def process_mongo_collection(db, api):
    logging.info(api['api_unique_id'] + ': Migration started')
    db_api = get_data_from_source(db, api['api_name'])
    if(db_api is not None):
        logging.info(api['api_unique_id'] + ': data fetched')
        preprocessing(api)
        logging.info(api['api_unique_id'] + ": Preprocessing done")
        try:
            processed_api = process_data_from_source(db_api=db_api, api=api)
            save_data_to_destination(db=db, processed_api=processed_api, partition=api['partition_for_parquet'])
        except:
            logging.error(api['api_unique_id'] + ": caught some error while migrating chunk.")

    logging.info(api['api_unique_id'] + ": Migration ended.\n")
