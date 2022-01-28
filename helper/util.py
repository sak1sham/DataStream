import pandas as pd
import json
import datetime
from os.path import exists
from os import makedirs
import logging

logging.basicConfig(filename="production.log",format='%(asctime)s %(message)s',filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def create_directories(list_databases):
    '''
        Creates the directories to save converted files
    '''
    if not exists("./converted"):
        makedirs("./converted")

    for db in list_databases:
        db_name = db['db_name']
        if not exists("./converted/" + db_name):
            makedirs("./converted/" + db_name)



def convert_list_to_string(l):
    '''
        Recursively convert lists and nested lists to strings
        Input: list
        Output: stringified list: str
    '''
    val = "["
    for item in l:
        if(isinstance(item, list) or isinstance(item, set) or isinstance(item, tuple)):
            item = convert_list_to_string(list(item))
        elif(isinstance(item, bool) or isinstance(item, float) or isinstance(item, complex) or isinstance(item, int)):
            item = str(item)
        elif(isinstance(item, dict)):
            item = json.dumps(item)
        elif(isinstance(item, datetime.datetime)):
            item = item.strftime("%m/%d/%Y, %H:%M:%S")
        val = val + item
    val = val + ']'
    return val



def convert_to_type(x, tp):
    '''
        Converts the variable 'x' to the type 'tp' defined by user in fields of migration_mapping
        INPUT:
            x: a variable
            tp: str containing destined datatype
        OUTPUT:
            x: data type as tp
    '''
    if((isinstance(x, str) and tp == 'string') or (isinstance(x, bool) and tp=='bool') or (isinstance(x, int) and tp=='integer') or (isinstance(x, float) and tp == 'float') or (isinstance(x, complex) and tp == 'complex')):
        return x
    if((isinstance(x, int) or isinstance(x, bool) or isinstance(x, float) or isinstance(x, complex)) and tp == 'string'):
        return str(x)
    if(isinstance(x, str) and tp == 'integer'):
        return int(x)
    if(isinstance(x, str) and tp=='bool'):
        return x=='True'
    if(isinstance(x, bool) and tp=='integer'):
        return int(x)
    if(isinstance(x, int) and tp=='bool'):
        return bool(x)
    if(isinstance(x, list)):
        return convert_list_to_string(x)
    if(isinstance(x, int) or isinstance(x, float) or isinstance(x, complex)):
        return str(x)
    if(isinstance(x, datetime.datetime) and tp=='string'):
            return x.strftime("%m/%d/%Y, %H:%M:%S")
    return x



def dataframe_from_collection(current_collection_name, collection_format={}):
    '''
        Converts the unstructed database documents to structured pandas dataframe
        INPUT:
            current_collection_name: MongoDB collection object to be converted
            collection_format: User-defined fields from migration_mapping for the collection
        OUTPUT:
            Pandas dataframe containing data of current_collection_name.
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
            
    return pd.DataFrame(docu)



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

