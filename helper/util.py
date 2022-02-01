import json
import datetime

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


def evaluate_cron(expression):
    '''
        order of values:
            year, month, day, week, day_of_week, hour, minute, second
    '''
    vals = expression.split()
    vals = [(None if w == '?' else w) for w in vals]
    return vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], vals[7]
