import pandas as pd
import json
import datetime
from helper.logger import logger

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
    ret_df = pd.DataFrame(docu)
    logger.info("Converted all " + str(total_len) + " documents to dataframe object")        
    return ret_df

def evaluate_cron(expression):
    '''
        order of values:
            year, month, day, week, day_of_week, hour, minute, second
    '''
    vals = expression.split()
    vals = [(None if w == '?' else w) for w in vals]
    return vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], vals[7]
