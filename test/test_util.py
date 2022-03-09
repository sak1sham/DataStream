import json
import datetime
import pytz
from typing import List, Dict, Any, NewType, Tuple
import pandas as pd

datetype = NewType("datetype", datetime.datetime)
dftype = NewType("dftype", pd.DataFrame)

std_datetime_format = "%Y/%m/%dT%H:%M:%S"

def convert_to_str(x) -> str:
    if(isinstance(x, int) or isinstance(x, float) or isinstance(x, str) or isinstance(x, bool)):
        return str(x)
    elif(isinstance(x, list)):
        return convert_list_to_string(x)
    elif(isinstance(x, dict)):
        return convert_json_to_string(x)
    elif(isinstance(x, datetime.datetime)):
        x = convert_to_datetime(x)
        return x.strftime("%Y/%m/%dT%H:%M:%S")
    else:
        return str(x)

def convert_list_to_string(l: List[Any]) -> str:
    '''
        Recursively convert lists and nested lists to strings
        Input: list
        Output: stringified list: str
    '''
    if(l is None):
        return None
    val = "["
    for item in l:
        if(isinstance(item, list) or isinstance(item, set) or isinstance(item, tuple)):
            item = convert_list_to_string(list(item))
        elif(isinstance(item, bool) or isinstance(item, float) or isinstance(item, complex) or isinstance(item, int)):
            item = str(item)
        elif(isinstance(item, dict)):
            item = json.dumps(item)
        elif(isinstance(item, datetime.datetime)):
            item = item.strftime(std_datetime_format)
        else:
            item = str(item)
        val = val + item + ", "
    if(len(val) > 1):
        val = val[:-2]
    val = val + ']'
    return val

def utc_to_local(utc_dt: datetype = None, tz_: Any = pytz.utc) -> datetype:
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(tz_)
    return tz_.normalize(local_dt)

def convert_to_utc(dt: datetype = None) -> datetype:
    if(dt.tzinfo is None):
        dt = pytz.utc.localize(dt)
    dt = dt.astimezone(pytz.utc)
    return dt

def convert_to_datetime(x: Any = None, tz_: Any = pytz.utc) -> datetype:
    if(x is None or x == pd.Timestamp(None) or x is pd.NaT):
        return pd.Timestamp(None)
    elif(isinstance(x, datetime.datetime)):
        x = convert_to_utc(dt = x)
        x = utc_to_local(utc_dt = x, tz_ = tz_)
        x = x.strftime(std_datetime_format)
        x = pd.to_datetime(x, utc=True)
        return x
    elif(isinstance(x, int) or isinstance(x, float)):
        x = datetime.datetime.fromtimestamp(x, pytz.utc)
        x = utc_to_local(utc_dt = x, tz_ = tz_)
        x = x.strftime(std_datetime_format)
        return pd.to_datetime(x, utc=True)
    else:
        try:
            x = pd.to_datetime(x, utc = True)
            return x
        except Exception as e:
            return pd.Timestamp(None)

def convert_json_to_string(x: Dict[str, Any]) -> str:
    '''
        Recursively convert json and nested json objects to strings
        Input: dict
        Output: stringified dict: str
    '''
    if(x is None):
        return x
    for item, value in x.items():
        if(isinstance(value, list) or isinstance(value, set) or isinstance(value, tuple)):
            x[item] = convert_list_to_string(list(x[item]))
        elif(isinstance(value, bool) or isinstance(value, float) or isinstance(value, complex) or isinstance(value, int)):
            x[item] = str(x[item])
        elif(isinstance(value, dict)):
            x[item] = convert_json_to_string(x[item])
        elif(isinstance(value, datetime.datetime)):
            x[item] = x[item].strftime(std_datetime_format)
        else:
            x[item] = str(x[item])
    return json.dumps(x)

def evaluate_cron(expression: str) -> List[str]:
    '''
        order of values:
            year, month, day, week, day_of_week, hour, minute, second
    '''
    if(expression is None):
        expression =  '1602 * * * * * */5 0'
    vals = expression.split()
    vals = [(None if w == '?' else w) for w in vals]
    return vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], vals[7]

def validate_or_convert(docu_orig: Dict[str, Any] = {}, schema: Dict[str, str] = {}, tz_info: Any = pytz.utc) -> Dict[str, Any]:
    docu = docu_orig.copy()
    for key, _ in docu.items():
        if(key == '_id'):
            docu[key] = str(docu[key])
        elif(isinstance(docu[key], list)):
            docu[key] = convert_list_to_string(docu[key])
        elif(isinstance(docu[key], dict)):
            docu[key] = convert_json_to_string(docu[key])
        elif(key in schema.keys()):
            if(schema[key] == 'int'):
                try:
                    docu[key] = int(float(docu[key]))
                except Exception as e:
                    docu[key] = 0
            elif(schema[key] == 'float'):
                try:
                    docu[key] = float(docu[key])
                except Exception as e:
                    docu[key] = None
            elif(schema[key] == 'datetime'):
                docu[key] = convert_to_datetime(docu[key], tz_info)
            elif(schema[key] == 'bool'):
                docu[key] = str(docu[key])
                docu[key] = bool(docu[key].lower() in ['true', '1', 't', 'y', 'yes'])
            elif(schema[key] == 'complex'):
                try:
                    docu[key] = complex(docu[key])
                except Exception as e:
                    docu[key] = None
        elif(isinstance(docu[key], datetime.datetime)):
            docu[key] = utc_to_local(docu[key], tz_info)
            docu[key] = docu[key].strftime(std_datetime_format)
        else:
            try:
                docu[key] = str(docu[key])
            except Exception as e:
                docu[key] = None
    
    for key, _ in schema.items():
        if(key not in docu.keys()):
            if(schema[key] == 'int'):
                docu[key] = 0
            elif(schema[key] == 'float'):
                docu[key] = None
            elif(schema[key] == 'datetime'):
                docu[key] = pd.Timestamp(None)
            elif(schema[key] == 'bool'):
                docu[key] = False
            elif(schema[key] == 'complex'):
                docu[key] = None
            else:
                docu[key] = None
    return docu

def typecast_df_to_schema(df: dftype, schema: Dict[str, Any]) -> dftype:
    for col in df.columns.values.tolist():
        tp = 'str'
        if(col in schema.keys()):
            tp = schema[col]
        if(tp == 'int'):
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        elif(tp == 'float'):
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
        elif(tp == 'complex'):
            df[col] = df[col].astype('|S')
        elif(tp == 'datetime'):
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).apply(lambda x: pd.Timestamp(x))
        elif(tp == 'bool'):
            df[col] = df[col].astype(bool)
        else:
            df[col] = df[col].astype(str)
    if(df.shape[0]):
        df = df.reindex(sorted(df.columns), axis=1)
    return df

def convert_jsonb_to_string(x: Any) -> str:
    if(isinstance(x, list)):
        return convert_list_to_string(x)
    elif(isinstance(x, dict)):
        return convert_json_to_string(x)
    else:
        try:
            x = str(x)
            return x
        except Exception as e:
            return None

def convert_range_to_str(r) -> str:
    if(r.isempty):
        return ""
    left = "None"
    if(not r.lower_inf):
        left = str(r.lower)
    right = "None"
    if(not r.upper_inf):
        right = str(r.upper)
    l1 = '('
    if(not r.lower_inf and r.lower_inc):
        l1 = '['
    r1 = ')'
    if(not r.upper_inf and r.upper_inc):
        r1 = ']'
    ret = l1 + left + ", " + right + r1
    return ret


def convert_to_dtype(df: dftype, schema: Dict[str, Any]) -> dftype:
    if(df.shape[0]):
        for col in df.columns.tolist():
            if(col in schema.keys()):
                dtype = schema[col].lower()
                if(dtype == 'jsonb' or dtype == 'json'):
                    df[col] = df[col].apply(lambda x: convert_jsonb_to_string(x))
                    df[col] = df[col].astype(str)
                elif(dtype.startswith('timestamp') or dtype.startswith('date')):
                    df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).apply(lambda x: pd.Timestamp(x))
                elif(dtype == 'boolean' or dtype == 'bool'):
                    df[col] = df[col].astype(bool)
                elif(dtype == 'bigint' or dtype == 'integer' or dtype == 'smallint' or dtype == 'bigserial' or dtype == 'smallserial' or dtype.startswith('serial') or dtype.startswith('int')):
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                elif(dtype == 'double precision' or dtype.startswith('numeric') or dtype == 'real' or dtype == 'double' or dtype == 'money' or dtype.startswith('decimal') or dtype.startswith('float')):
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
                elif(dtype == 'cidr' or dtype == 'inet' or dtype == 'macaddr' or dtype == 'uuid' or dtype == 'xml'):
                    df[col] = df[col].astype(str)
                elif('range' in dtype):
                    df[col] = df[col].apply(convert_range_to_str).astype(str)
                elif('interval' in dtype):
                    df[col] = df[col].astype(str)
                else:
                    df[col] = df[col].astype(str)
            else:
                df[col] = df[col].astype(str)
    return df
    
def df_upsert(df: dftype = pd.DataFrame({}), df_u: dftype = pd.DataFrame({}), primary_key: str = None) -> Tuple[dftype, bool]:
    '''
        While upserting the data, we will be having only one record in df_u, as we are upserting record by record in s3
    '''
    pkey = df_u.iloc[0][primary_key]
    modify = pkey in df[primary_key].values
    if(modify):
        final_df = df.merge(df_u, on = primary_key, how = 'outer', suffixes=('', '_dms'))
        final_df.drop(list(final_df.filter(regex=r'.*_dms$').columns), axis=1, inplace=True)
        return final_df, True
    else:
        return df, False

def get_athena_dtypes(maps: Dict[str, str] = {}) -> Dict[str, str]:
    athena_types = {}
    for key, dtype in maps.items():
        if(dtype == 'str' or dtype == 'string' or dtype == 'jsonb' or dtype == 'json' or dtype == 'cidr' or dtype == 'inet' or dtype == 'macaddr' or dtype == 'uuid' or dtype == 'xml' or 'range' in dtype or 'interval' in dtype):
            athena_types[key] = 'string'
        elif(dtype == 'datetime' or dtype.startswith('timestamp') or dtype.startswith('date')):
            athena_types[key] = 'timestamp'
        elif(dtype == 'bool' or dtype == 'boolean'):
            athena_types[key] = 'boolean'
        elif(dtype == 'int' or dtype == 'bigint' or dtype == 'integer' or dtype == 'smallint' or dtype == 'bigserial' or dtype == 'smallserial' or dtype == 'serial' or dtype.startswith('serial') or dtype.startswith('int')):
            athena_types[key] = 'bigint'
        elif(dtype == 'float' or dtype == 'double precision' or dtype.startswith('numeric') or dtype == 'real' or dtype == 'double' or dtype == 'money' or dtype.startswith('decimal') or dtype.startswith('float')):
            athena_types[key] = 'float'
    return athena_types