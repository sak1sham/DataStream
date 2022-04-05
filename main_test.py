from config.migration_mapping import *
from testing.pgsql import SqlTester
#from testing.mongo import MongoTester
import unittest
import sys
from dotenv import load_dotenv
from config.migration_mapping import get_mapping
load_dotenv()
if __name__ == "__main__":
    N = 200
    id = ''
    if(len(sys.argv) > 1):
        id = sys.argv.pop()
    mapping = get_mapping(id)
    if('tables' not in mapping.keys()):
        mapping['tables'] = []
    for table in mapping['tables']:
        print("Testing", table['table_name'])
        SqlTester.N = N
        SqlTester.url = mapping['source']['url']
        SqlTester.db = mapping
        SqlTester.id_ = id + "_DMS_" + table['table_name']
        SqlTester.table = table['table_name']
        SqlTester.table_map = table
        if 'primary_key' in table.keys():
            SqlTester.primary_key = table['primary_key']
        unittest.main(exit=False, warnings='ignore')   