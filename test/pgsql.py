from logging import exception
from math import log
from numpy import dtype
import psycopg2
import awswrangler as wr
import unittest
import sys
import math
import random
import datetime
from typing import NewType
import pytz

from pyparsing import col
datetype = NewType("datetype", datetime.datetime)

from dotenv import load_dotenv
load_dotenv()

from testing_mapping import mapping
from test_util import *

class SqlTester(unittest.TestCase):
    id_ = ''
    url = ''
    db = ''
    table = ''
    test_N = 2
    table_map = {}
    primary_keys = []
    tz_info = pytz.timezone("Asia/Kolkata")
    # def confidence(self, N: int = 10):
    #     percent = float(95.0 + 0.5 * log(N, 10))
    #     return percent/100.0

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
        
        if(N > 0):
            athena_table = str(self.table).replace('.', '_').replace('-', '_')
            query = 'SELECT COUNT(*) as count FROM ' + athena_table + ';'
            database = "sql" + "_" + self.db['source']['db_name'].replace('.', '_').replace('-', '_')
            df = wr.athena.read_sql_query(sql = query, database = database)
            athena_count = int(df.iloc[0]['count'])
            assert athena_count >= int(confidence(N) * N)
            assert athena_count <= N
        print("Count Test completed")
        
    # https://stackoverflow.com/questions/580639/how-to-randomly-select-rows-in-sql
    # Select RANDOM RECORDS from PgSQL

    def get_column_dtypes(self, conn: Any = None, curr_table_name: str = None) -> Dict[str, str]:
        tn = curr_table_name.split('.')
        schema_name = 'public'
        table_name = ''
        if(len(tn) > 1):
            schema_name = tn[0]
            table_name = tn[1]
        else:
            # if table_name doesn't contain schema name
            table_name = tn[0]
        sql_stmt = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = \'" + table_name + "\' AND table_schema = \'" + schema_name + "\';"
        col_dtypes = {}
        with conn.cursor('cursor-to-get-col-names') as curs:
            curs.execute(sql_stmt)
            rows = curs.fetchall()
            for key, val in rows:
                col_dtypes[key] = val
        if(self.table_map['mode'] == 'dumping'):
            col_dtypes['migration_snapshot_date'] = 'datetime'
        return col_dtypes

    def check_match(self, record, athena_record, column_dtypes) -> bool:
        for key in record.keys():
            athena_key = key.lower()
                
            if not key.startswith('parquet_format'):
                if column_dtypes[key].startswith('timestamp') or column_dtypes[key].startswith('date'):
                    if record[key] is not pd.NaT and athena_record[athena_key] is not pd.NaT:
                        athena_record[athena_key] = int((pytz.utc.localize(athena_record[athena_key])).timestamp())
                        record[key] = int((record[key]).timestamp())
                    else:
                        athena_record[athena_key] = str(athena_record[athena_key])
                        record[key] = str(record[key])
            else:
                athena_record[athena_key] = str(athena_record[athena_key])
                record[key] = str(record[key])
            assert record[key] == athena_record[athena_key]
        return True

    def add_partitions(self, df: dftype = pd.DataFrame({})) -> dftype:
        self.partition_for_parquet = []
        if('partition_col' in self.table_map.keys()):
            if(isinstance(self.table_map['partition_col'], str)):
                self.table_map['partition_col'] = [self.table_map['partition_col']]
            if('partition_col_format' not in self.table_map.keys()):
                self.table_map['partition_col_format'] = ['str']
            if(isinstance(self.table_map['partition_col_format'], str)):
                self.table_map['partition_col_format'] = [self.table_map['partition_col_format']]
            while(len(self.table_map['partition_col']) > len(self.table_map['partition_col_format'])):
                self.table_map['partition_col_format'] = self.table_map['partition_col_format'].append('str')
            for i in range(len(self.table_map['partition_col'])):
                col = self.table_map['partition_col'][i].lower()
                col_form = self.table_map['partition_col_format'][i]
                parq_col = "parquet_format_" + col
                if(col == 'migration_snapshot_date' or col_form == 'datetime'):
                    self.partition_for_parquet.extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                    temp = df[col].apply(lambda x: convert_to_datetime(x, self.tz_info))
                    df[parq_col + "_year"] = temp.dt.year
                    df[parq_col + "_month"] = temp.dt.month
                    df[parq_col + "_day"] = temp.dt.day
                elif(col_form == 'str'):
                    self.partition_for_parquet.extend([parq_col])
                    df[parq_col] = df[col].astype(str)
                elif(col_form == 'int'):
                    self.partition_for_parquet.extend([parq_col])
                    df[parq_col] = df[col].astype(int)
        return df

    def test_pgsql(self):
        conn = psycopg2.connect(
            host = self.db['source']['url'],
            database = self.db['source']['db_name'],
            user = self.db['source']['username'],
            password = self.db['source']['password']
        )
        column_dtypes = self.get_column_dtypes(conn=conn, curr_table_name=self.table)
        sql_stmt = "SELECT * FROM {0} ORDER BY RANDOM() LIMIT {1}".format(self.table, self.test_N)
        data_df = pd.DataFrame({})
        with conn.cursor('test-cursor-name', scrollable=True) as curs:
            curs.execute(sql_stmt)
            _ = curs.fetchone()
            columns = [desc[0] for desc in curs.description]
            curs.scroll(-1)
            ret = curs.fetchall()    
            if ret:
                data_df = pd.DataFrame(ret, columns = columns)
        ## Adding a primary key "unique_migration_record_id" for every record
        self.add_partitions(data_df)
        if len(self.primary_keys) == 0:
            self.primary_keys = data_df.columns.values.tolist()
        if(isinstance(self.primary_keys, str)):
            self.primary_keys = [self.primary_keys]
        self.primary_keys = [x.lower() for x in self.primary_keys]
        data_df['unique_migration_record_id'] = data_df[self.primary_keys].astype(str).sum(1)
        column_dtypes['unique_migration_record_id'] = 'str'
        convert_to_dtype(data_df, column_dtypes)
        data_df = data_df.to_dict(orient='records')
        athena_table = str(self.table).replace('.', '_').replace('-', '_')
        for row in data_df:
            query = 'SELECT * FROM ' + athena_table + ' WHERE unique_migration_record_id = \'' + str(row['unique_migration_record_id']) + '\';'
            # query = 'SELECT * FROM ' + athena_table + ' WHERE id = ' + str(row['id']) + ';'
            database = "sql" + "_" + self.db['source']['db_name'].replace('.', '_').replace('-', '_')
            df = wr.athena.read_sql_query(sql = query, database = database)
            athena_record = df[0:1].to_dict(orient='records')
            assert self.check_match(row, athena_record[0], column_dtypes)
            print("Record Tested")
        print("PGSQL Test Completed")


if __name__ == "__main__":
    id = ''
    if (len(sys.argv) > 1):
        id = sys.argv.pop()
    if('tables' not in mapping[id].keys()):
        mapping[id]['tables'] = []
    for table in mapping[id]['tables']:
        print("Testing", table['table_name'])
        SqlTester.url = mapping[id]['source']['url']
        SqlTester.db = mapping[id]
        SqlTester.id_ = id + "_DMS_" + table['table_name']
        SqlTester.table = table['table_name']
        SqlTester.table_map = table
        if 'primary_keys' in table.keys():
            SqlTester.primary_keys = table['primary_keys']
        unittest.main(exit=False, warnings='ignore')

'''

python accuracy.py mongo_support_service_to_s3_DMS_support_form_items 'mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservicev2.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority' support_service support_form_items

'''