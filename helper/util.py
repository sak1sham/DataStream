import json
import datetime

import logging
logging.getLogger().setLevel(logging.INFO)

import pytz
from dateutil import parser

from typing import List, Dict, Any, NewType
import pandas as pd

datetype = NewType("datetype", datetime.datetime)
dftype = NewType("datetype", pd.DataFrame)

std_datetime_format = "%Y/%m/%dT%H:%M:%S"


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
        val = val + item
    val = val + ']'
    return val

def utc_to_local(utc_dt: datetype, tz_: Any) -> datetype:
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(tz_)
    return tz_.normalize(local_dt)

def convert_to_datetime(x: Any, tz_: Any) -> datetype:
    if(x is None):
        return pd.Timestamp(None)
    elif(isinstance(x, datetime.datetime)):
        return utc_to_local(x, tz_)
    else:
        try:
            x = parser.parse(x)
            return utc_to_local(x, tz_)
        except:
            logging.warning("Unable to convert " + x + " to any datetime format. Returning None")
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
            x[item] = json.dumps(x[item])
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
        logging.warning("Cron Expression Not Specified. Unable to run job")
        expression =  '1602 * * * * * */5 0'
    vals = expression.split()
    vals = [(None if w == '?' else w) for w in vals]
    return vals[0], vals[1], vals[2], vals[3], vals[4], vals[5], vals[6], vals[7]

def validate_or_convert(docu: Dict[str, Any], schema: Dict[str, str], tz_info: Any) -> Dict[str, Any]:
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
                    docu[key] = int(docu[key])
                except:
                    docu[key] = 0
            elif(schema[key] == 'float'):
                try:
                    docu[key] = float(docu[key])
                except:
                    docu[key] = None
            elif(schema[key] == 'datetime'):
                docu[key] = convert_to_datetime(docu[key], tz_info)
            elif(schema[key] == 'bool'):
                try:
                    docu[key] = bool(docu[key])
                except:
                    docu[key] = docu[key].lower() in ['true', '1', 't', 'y', 'yes']
            elif(schema[key] == 'complex'):
                try:
                    docu[key] = complex(docu[key])
                except:
                    docu[key] = None
        elif(isinstance(docu[key], datetime.datetime)):
            docu[key] = utc_to_local(docu[key], tz_info)
            docu[key] = docu[key].strftime("%Y-%m-%dT%H:%M:%S")
        else:
            try:
                docu[key] = str(docu[key])
            except:
                logging.warning("Unidentified datatype at docu _id:" + str(docu['_id']) + ". Saving NoneType.")
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
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
        elif(tp == 'float'):
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
        elif(tp == 'complex'):
            df[col] = df[col].astype('|S')
        elif(tp == 'datetime'):
            #df[col] = pd.to_datetime(df[col], utc=False).dt.tz_convert(pytz.utc).apply(lambda x: pd.Timestamp(x))
            df[col] = pd.to_datetime(df[col], utc=False).dt.tz_convert(pytz.utc).apply(lambda x: x.strftime(std_datetime_format) if not pd.isnull(x) else '')
            df[col] = df[col].astype('|S')
        elif(tp == 'bool'):
            df[col] = df[col].astype(bool)
        else:
            df[col] = df[col].astype(str)
    return df