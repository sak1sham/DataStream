from pymongo import MongoClient
from helper.util import dataframe_from_collection
from helper.logger import logger
import certifi


def fetch_and_convert_data(database_, fetch_type = "selected", collection_name = [], db_name=""):
    '''
        Inputs:
            database_: PyMongo database object
            fetch_type: 'all' (for all collections), 'selected' (for specific collections)
            collection_name: List (all collection names and user-defined specs)

        Output:
            Returns nothing if success
            Error handling: returns the pd.dataframe in case of incompatibility with parquet

    '''

    ## Handle the case: fetch_type=='all'; but some collections are not provided by user
    if(fetch_type == 'all'):
        cnames = database_.list_collection_names()
        cnames2 = []
        for curr_collection in collection_name:
            cnames2.append(curr_collection['collection_name'])
        cnames = list(set(cnames)-set(cnames2))
        for coll in cnames:
            collection_name.append({'collection_name': coll})

    ## All required collections are present in collection_name variable
    ## Now, iterate through all the collections, fetch the data, convert to pandas dataframe to parquet
    for curr_collection in collection_name:
        ## Add empty 'field' parameter if not provided by user
        if('fields' not in curr_collection.keys()):
            curr_collection['fields'] = {}

        df_collection = dataframe_from_collection(database_[curr_collection['collection_name']], collection_format=curr_collection['fields'])
        
        assert len(db_name) > 0
        file_name = "./converted/" + db_name + "/" + curr_collection['collection_name'] + '.parquet'
        
        df_collection.to_parquet(file_name, engine='pyarrow', compression='snappy', partition_cols=['parquet_format_date_year', 'parquet_format_date_month'])
        logger.info("Saved" + file_name)



def process_db(db):
    print('Time to connect to db', db['db_name'])
    client = MongoClient(db['url'], tlsCAFile=certifi.where())
    if('collections' not in db.keys()):
        db['collections'] = []
    fetch_and_convert_data(client[db['db_name']], fetch_type=db['fetch_type'], collection_name=db['collections'], db_name=db['db_name'])
