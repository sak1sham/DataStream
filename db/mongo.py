from pymongo import MongoClient
from helper.util import convert_list_to_string, convert_to_type
from helper.logger import logger
import certifi
from dst.s3 import save_to_s3
import pandas as pd
import datetime

def dataframe_from_collection(current_collection_name, collection_format={}):
    '''
        Converts the unstructed database documents to structured pandas dataframe
    '''
    docu = []
    count = 0
    total_len = current_collection_name.count_documents({})
    logger.info("Found " + str(total_len) + " documents.")
    for document in current_collection_name.find():
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
        docu.append(document)
        if(count % 10000 == 0):
            logger.info(str(count)+ " documents fetched ... " + str(int(count*100/total_len)) + " %")
    ret_df = pd.DataFrame(docu)
    logger.info("Converted all " + str(total_len) + " documents to dataframe object")        
    return ret_df

def get_data_from_source(db):
    client = MongoClient(db['url'], tlsCAFile=certifi.where())
    if('collections' not in db.keys()):
        db['collections'] = []
    database_ = client[db['db_name']]
    return database_

def process_data_from_source(database_, collection_name = []):
    df_list = []
    for curr_collection in collection_name:
        if('fields' not in curr_collection.keys()):
            curr_collection['fields'] = {}
        df_collection = dataframe_from_collection(database_[curr_collection['collection_name']], collection_format=curr_collection['fields'])
        df_list.append({'collection_name': curr_collection['collection_name'], 'df_collection': df_collection})
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
