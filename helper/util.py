import json
import datetime
from helper.logger import log_writer

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
        else:
            item = str(item)
        val = val + item
    val = val + ']'
    return val

def convert_to_type(x, tp):
    '''
        Converts the variable 'x' to the type 'tp' defined by user in fields of migration_mapping
        INPUT:
            x: a variable
            tp: str containing desired datatype
        OUTPUT:
            x: data type as tp
    '''
    if(tp == 'bool'):
        if(isinstance(x, bool)):
            return x
        elif(isinstance(x, int) or isinstance(x, complex) or isinstance(x, float)):
            return x > 0
        elif(isinstance(x, str)):
            x = x.lower() in ['true', '1', 't', 'y', 'yes']
            return x
        else:
            log_writer("Unable to convert " + str(type(x)) + " to boolean. Returning true.")
            return True
    elif(tp == 'int'):
        try:
            return int(x)
        except:
            log_writer("Unable to convert " + str(type(x)) + " to int. Returning 1")
            return 1
    elif(tp == 'float'):
        try:
            return float(x)
        except:
            log_writer("Unable to convert " + str(type(x)) + " to float. Returning 1.0")
            return 1.0
    elif(tp == 'complex'):
        try:
            return complex(x)
        except:
            log_writer("Unable to convert " + str(type(x)) + " to complex. Returning 1+0j")
            return 1+0j
    elif(tp == "datetime"):
        if(isinstance(x, datetime.datetime)):
            return x
        else:
            log_writer("Unable to convert " + str(type(x)) + " to datetime. Returning datetime.datetime(1602, 8, 20, 0, 0, 0, 0)")
            return datetime.datetime(1602, 8, 20, 0, 0, 0, 0)
    else:
        # Convert to string
        if(isinstance(x, datetime.datetime)):
            return x.strftime("%m/%d/%Y, %H:%M:%S")
        elif(isinstance(x, dict)):
            return convert_json_to_string(x)
        elif(isinstance(x, list)):
            return convert_list_to_string(x)
        else:
            try:
                return str(x)
            except:
                log_writer("Unable to convert " + str(type(x)) + " to string. Returning empty string \"\"")
                return ""

def convert_to_datetime(x, format):
    if(isinstance(x, datetime.datetime)):
        return x
    try:
        x = datetime.datetime.strptime(x, format)
        return x
    except:
        log_writer("Unable to convert " + x + " to format specified: \"" + format + "\". Returning 20-8-1602 00:00:00")
        return datetime.datetime(1602, 8, 20, 0, 0, 0, 0)

def convert_json_to_string(x):
    '''
        Recursively convert json and nested json objects to strings
        Input: dict
        Output: stringified dict: str
    '''
    for item, value in x.items():
        if(isinstance(value, list) or isinstance(value, set) or isinstance(value, tuple)):
            x[item] = convert_list_to_string(list(x[item]))
        elif(isinstance(value, bool) or isinstance(value, float) or isinstance(value, complex) or isinstance(value, int)):
            x[item] = str(x[item])
        elif(isinstance(value, dict)):
            x[item] = json.dumps(x[item])
        elif(isinstance(value, datetime.datetime)):
            x[item] = x[item].strftime("%m/%d/%Y, %H:%M:%S")
        else:
            x[item] = str(x[item])
    return json.dumps(x)

def evaluate_cron(expression):
    '''
        order of values:
            year, month, day, week, day_of_week, hour, minute, second
    '''
    vals = expression.split()
    vals = [(None if w == '?' else w) for w in vals]
    return vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], vals[7]
