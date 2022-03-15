from math import log
from pymongo import MongoClient
import certifi
import awswrangler as wr
import unittest
import sys
import math
import random
import datetime
from typing import NewType
datetype = NewType("datetype", datetime.datetime)

from dotenv import load_dotenv
load_dotenv()

from testing_mapping import mapping
from test_util import *

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

class MongoTester(unittest.TestCase):
    id_ = ''
    url = ''
    db = ''
    col = ''
    test_N = 100
    col_map = {}

    # def confidence(self, N: int = 10):
    #     percent = float(95.0 + 0.5 * log(N, 10))
    #     return percent/100.0

    def test_count(self):
        client_encr = MongoClient(self.url, tlsCAFile=certifi.where())
        db_encr = client_encr[self.db]
        collection = db_encr[self.col]
        N = collection.count_documents({})
        if(N > 0):
            query = 'SELECT COUNT(*) as count FROM ' + self.col + ';'
            df = wr.athena.read_sql_query(sql = query, database = "mongo" + "_" + self.db.replace('.', '_').replace('-', '_'))
            athena_count = int(df.iloc[0]['count'])
            assert athena_count >= int(confidence(N) * N)
            assert athena_count <= N

    def check_match(self, record, athena_record) -> bool:
            for key in record.keys():
                athena_key = key.lower()
                if(key == '_id'):
                    assert str(record[key]) == athena_record[athena_key]
                elif(key in self.col_map['fields'].keys()):
                    val = self.col_map['fields'][key]
                    if(val == 'int'):
                        assert int(float(record[key])) == athena_record[athena_key] or athena_record[athena_key] == 0
                    elif(val == 'float'):
                        assert float(record[key]) == athena_record[key] or athena_record[athena_key] is None
                    elif(val == 'bool'):
                        record[key] = str(record[key])
                        assert (record[key].lower() in ['true', '1', 't', 'y', 'yes'] and athena_record[athena_key]) or (not athena_record[key])
                    elif(val == 'datetime'):
                        assert abs((convert_to_datetime(record[key])-convert_to_datetime(athena_record[athena_key])).total_seconds()) <= 60
                    else:
                        assert convert_to_str(record[key]) == athena_record[athena_key]
                else:
                    assert convert_to_str(record[key]) == athena_record[athena_key]
            return True

    def test_mongo(self):
        client_encr = MongoClient(self.url, tlsCAFile=certifi.where())
        db_encr = client_encr[self.db]
        collection = db_encr[self.col]
        N = collection.count_documents({})
        curs = collection.find({}).limit(self.test_N).skip(math.floor(random.random()*N))
        curs = list(curs)
        for record in curs:
            query = 'SELECT * FROM ' + self.col + ' WHERE _id = \'' + str(record['_id']) + '\';'
            df = wr.athena.read_sql_query(sql = query, database = "mongo" + "_" + self.db.replace('.', '_').replace('-', '_'))
            athena_record = df[0:1].to_dict(orient='records')
            assert self.check_match(record, athena_record[0])

if __name__ == "__main__":
    id = ''
    if (len(sys.argv) > 1):
        id = sys.argv.pop()
    if('collections' not in mapping[id].keys()):
        mapping[id]['collections'] = []
    for col in mapping[id]['collections']:
        print("Testing", col['collection_name'])
        MongoTester.url = mapping[id]['source']['url']
        MongoTester.db = mapping[id]['source']['db_name']
        MongoTester.id_ = id + "_DMS_" + col['collection_name']
        MongoTester.col = col['collection_name']
        MongoTester.col_map = col
        unittest.main(exit=False, warnings='ignore')

'''

python accuracy.py mongo_support_service_to_s3_DMS_support_form_items 'mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservicev2.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority' support_service support_form_items

'''