from pymongo import MongoClient
from helper.util import dataframe_from_collection
from helper.logger import logger
import certifi
from dst.s3 import save_to_s3

def get_data_from_source(db):
    client = MongoClient(db['url'], tlsCAFile=certifi.where())
    if('collections' not in db.keys()):
        db['collections'] = []
    ## Handle the case: fetch_type=='all'; but some collection fields are defined by the user in migration_mapping
    if(db['fetch_type'] == 'all'):
        cnames = client[db['db_name']].list_collection_names()
        cnames2 = []
        for curr_collection in db['collections']:
            cnames2.append(curr_collection['collection_name'])
        cnames = list(set(cnames)-set(cnames2))
        for coll in cnames:
            db['collections'].append({'collection_name': coll})
    database_ = client[db['db_name']]
    return database_

def process_data_from_source(database_, collection_name = []):
    df_list = []
    for curr_collection in collection_name:
        ## Add empty 'field' parameter if not provided by user
        if('fields' not in curr_collection.keys()):
            curr_collection['fields'] = {}
        df_collection = dataframe_from_collection(database_[curr_collection['collection_name']], collection_format=curr_collection['fields'])
        df_list.append({'collection_name': curr_collection['collection_name'], 'df_collection': df_collection})
    return df_list
    
def save_data_to_destination(db, df_list):
    if(db['destination_type'] == 'local'):
        for dfl in df_list:
            file_name = './converted/mongo/' + db['db_name'] + "/" + dfl['collection_name'] + '.parquet'
            dfl['df_collection'].to_parquet(file_name, engine='pyarrow', compression='snappy', partition_cols=['parquet_format_date_year', 'parquet_format_date_month'])
            logger.info("Saved " + file_name)
            
    if(db['destination_type'] == 's3'):
        save_to_s3(df_list, db)

def process_mongodb(db):
    logger.info('Time to connect to db ' + db['db_name'])
    database_ = get_data_from_source(db)
    df_list = process_data_from_source(database_=database_, collection_name=db['collections'])
    save_data_to_destination(db=db, df_list=df_list)
