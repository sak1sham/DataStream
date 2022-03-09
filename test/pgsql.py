from math import log
import psycopg2
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

class SqlTester(unittest.TestCase):
    id_ = ''
    url = ''
    db = ''
    table = ''
    test_N = 100
    table_map = {}

    def confidence(self, N: int = 10):
        percent = float(95.0 + 0.5 * log(N, 10))
        return percent/100.0

    def test_count(self):
        sql_stmt = "SELECT COUNT(*) as count FROM " + self.table
        if('username' not in self.db['source'].keys()):
            self.db['source']['username'] = ''
        if('password' not in self.db['source'].keys()):
            self.db['source']['password'] = ''
        conn = psycopg2.connect(
            host = self.db['source']['url'],
            database = self.db['source']['db_name'],
            user = self.db['source']['username'],
            password = self.db['source']['password']
        )
        with conn.cursor('test-cursor-name') as curs:
            curs.execute(sql_stmt)
            ret = curs.fetchall()
            N = ret[0][0]
            print(N, type(N))
        
    # https://stackoverflow.com/questions/580639/how-to-randomly-select-rows-in-sql
    # Select RANDOM RECORDS from PgSQL


if __name__ == "__main__":
    id = ''
    if (len(sys.argv) > 1):
        id = sys.argv.pop()
    if('tables' not in mapping[id].keys()):
        mapping[id]['tables'] = []
    for table in mapping[id]['tables']:
        print("Testing", table['table_name'])
        SqlTester.url = mapping[id]['source']['url']
        SqlTester.db = mapping[id]['source']['db_name']
        SqlTester.id_ = id + "_DMS_" + table['table_name']
        SqlTester.table = table['table_name']
        SqlTester.table_map = table
        unittest.main(exit=False, warnings='ignore')

'''

python accuracy.py mongo_support_service_to_s3_DMS_support_form_items 'mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservicev2.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority' support_service support_form_items

'''