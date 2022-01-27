import pandas as pd
import time
import json
import datetime

def convert_list_to_string(l):
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
    docu = []
    count = 0
    total_len = current_collection_name.count_documents({})
    print("Found", total_len, "documents.")
    for document in current_collection_name.find():
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
            print(count, "Documents fetched.................", int(count*100/total_len), "%")
            
    return pd.DataFrame(docu)

def fetch_and_convert_data(database_, fetch_type = "selected", collection_name = []):
    '''
        Inputs:
            database_: the reference to the database object from pymongo connection
            fetch_type: 'all' for all collections and 'selected' for fetching some collections
            collection_name: List of all collection names to fetch, along with their formats.
        Output:
            Returns nothing if success
            Error handling: returns the pd.dataframe in case of incompatibility with parquet

    '''
    if(fetch_type == 'all'):
        '''
            Handle the case when fetch type is 'all' but only some of the collections are mentioneed by user
        '''
        cnames = database_.list_collection_names()
        cnames2 = []
        for curr_collection in collection_name:
            cnames2.append(curr_collection['collection_name'])
        cnames = list(set(cnames)-set(cnames2))
        for coll in cnames:
            collection_name.append({'collection_name': coll})

    '''
        Assert: All required collections are present in collection_name variable
        Now, iterate through all the collections and fetch the data converted into dataframe
        Later, convert it into parquet format
    '''
    for curr_collection in collection_name:
        if('format' not in curr_collection.keys()):
            curr_collection['format'] = {}
        df_collection = dataframe_from_collection(database_[curr_collection['collection_name']], collection_format=curr_collection['format'])
        print("Converted to Pandas Dataframe. Now, saving to parquet format")
        file_name = "./converted/" + curr_collection['collection_name'] + '.parquet'
        try:
            df_collection.to_parquet(file_name)
            print("Saved", file_name)
        except:
            return df_collection

