from config.migration_mapping import *
import unittest
import sys

from dotenv import load_dotenv
load_dotenv()

from config.migration_mapping import get_mapping

N = 200

def pgsql_test(id, mapping):
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

def mongo_test(id, mapping):
    if('collections' not in mapping.keys()):
        mapping['collections'] = []
    for col in mapping['collections']:
        print("Testing", col['collection_name'])
        MongoTester.url = mapping['source']['url']
        MongoTester.db = mapping['source']['db_name']
        MongoTester.id_ = id + "_DMS_" + col['collection_name']
        MongoTester.col = col['collection_name']
        MongoTester.col_map = col

if __name__ == "__main__":
    id = ''
    if(len(sys.argv) > 1):
        id = sys.argv.pop()
    mapping = get_mapping(id)
    if(mapping['source']['source_type'] == 'sql'):
        from testing.pgsql import SqlTester
        pgsql_test(id, mapping)
    elif(mapping['source']['source_type'] == 'mongo'):
        from testing.mongo import MongoTester
        mongo_test(id, mapping)    
    unittest.main(exit=False, warnings='ignore')