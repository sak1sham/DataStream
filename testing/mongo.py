from pymongo import MongoClient
import certifi
import awswrangler as wr
import unittest
import math
import random
import datetime
from typing import NewType
from bson import ObjectId
from dotenv import load_dotenv

load_dotenv()
import pytz
datetype = NewType("datetype", datetime.datetime)

from dotenv import load_dotenv
load_dotenv()

from testing.test_util import *

def convert_to_str(x) -> str:
    if(isinstance(x, list)):
        return convert_list_to_string(x)
    elif(isinstance(x, dict)):
        return convert_json_to_string(x)
    elif(isinstance(x, datetime.datetime)):
        x = convert_to_datetime(x)
        return x.strftime("%Y/%m/%dT%H:%M:%S")
    else:
        return str(x)

certificate = certifi.where()
certificate = 'config/rds-combined-ca-bundle.pem'

class MongoTester(unittest.TestCase):
    id_ = ''
    url = ''
    db = ''
    col = ''
    test_N = 2000
    col_map = {}
    tz_info = pytz.timezone('Asia/Kolkata')
    N_mongo = -1

    def get_last_run_cron_job(self):
        client_encr = MongoClient('mongodb+srv://manish:KlSh0bX605PY509h@cluster0.ebwdr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority', tlsCAFile=certifi.where())
        db_encr = client_encr['migration_update_check']
        collection_encr = db_encr['migration_update_check']
        curs = collection_encr.find({'last_run_cron_job_for_id': self.id_})
        curs = list(curs)
        return curs[0]['timing']

    def count_docs(self):
        if(self.N_mongo == -1):
            client = MongoClient(self.url, tlsCAFile=certificate)
            db = client[self.db]
            collection = db[self.col]
            self.N_mongo = collection.count_documents({})
        return self.N_mongo
    
    def abc_test_count(self):
        N = self.count_docs()
        if(N > 0):
            query = 'SELECT COUNT(*) as count FROM ' + self.col + ';'
            df = wr.athena.read_sql_query(sql = query, database = "mongo" + "_" + self.db.replace('.', '_').replace('-', '_'))
            athena_count = int(df.iloc[0]['count'])
            assert athena_count >= int(confidence(N) * N)
            assert athena_count <= N

    def check_match(self, record, athena_record) -> bool:
        try:
            for key in record.keys():
                if(record[key]):
                    try:
                        athena_key = key.lower()
                        if(key == '_id'):
                            assert str(record[key]) == athena_record[athena_key]
                        elif(key in self.col_map['fields'].keys()):
                            val = self.col_map['fields'][key]
                            if record[key]:
                                if(val == 'int'):
                                    assert int(float(record[key])) == athena_record[athena_key]
                                elif(val == 'float'):
                                    assert float(record[key]) == athena_record[key]
                                elif(val == 'bool'):
                                    record[key] = str(record[key])
                                    assert (record[key].lower() in ['true', '1', 't', 'y', 'yes'] and athena_record[athena_key]) or (record[key].lower() not in ['true', '1', 't', 'y', 'yes'] and not athena_record[athena_key])
                                elif(val == 'datetime'):
                                    date1 = convert_to_datetime(record[key])
                                    date2 = convert_to_datetime(athena_record[athena_key])
                                    assert (date1 is pd.NaT and date2 is pd.NaT) or (abs((date1-date2).total_seconds()) <= 1)
                                else:
                                    assert convert_to_str(record[key]) == athena_record[athena_key]
                        else:
                            assert convert_to_str(record[key]) == athena_record[athena_key]
                    except:
                        print(key)
                        print('record[key]', record[key], "of type", type(record[key]))
                        print('athena_record[athena_key]', athena_record[key.lower()], "of type", type(athena_record[key.lower()]))
                        print("\n\n\n\n\n")
                        raise
            return True
        except Exception as e:
            print(e)
            print(record['_id'])

    def test_mongo(self):
        client = MongoClient(self.url, tlsCAFile=certificate)
        db = client[self.db]
        collection = db[self.col]
        N = self.count_docs()
        prev_time = pytz.utc.localize(self.get_last_run_cron_job())
        last_run_cron_job_id = ObjectId.from_datetime(prev_time)
        query = {
            "_id": {
                "$lt": last_run_cron_job_id, 
            }
        }
        curs = collection.find(query).limit(self.test_N).skip(math.floor(random.random()*N))
        curs = list(curs)
        
        str_id = ""
        for records in curs:
            str_id += "\'" + str(records['_id']) + "\',"
        
        if(len(curs)):
            query = 'SELECT * FROM ' + self.col + ' WHERE _id in (' + str_id[:-1] + ');'
            database = "mongo" + "_" + self.db.replace('.', '_').replace('-', '_')
            df = wr.athena.read_sql_query(sql = query, database = database)
            for record in curs:
                if('bookmark' in self.col_map.keys() and self.col_map['bookmark']):
                    if('improper_bookmarks' in self.col_map.keys() and not self.col_map['improper_bookmarks']):
                        if(pytz.utc.localize(record[self.col_map['bookmark']]) > prev_time):
                            print('Record updated later.')
                            continue
                    else:
                        if(convert_to_datetime(record[self.col_map['bookmark']], pytz.utc) > prev_time):
                            print("Record updated later.")
                            continue
                athena_record = df.loc[df['_id'] == str(record['_id'])].to_dict(orient='records')
                assert self.check_match(record, athena_record[0])