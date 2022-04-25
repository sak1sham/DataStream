import psycopg2
import awswrangler as wr
import unittest
import datetime
from typing import NewType
import pytz
from pymongo import MongoClient
datetype = NewType("datetype", datetime.datetime)
import sys
import numpy

from test_util import *
from migration_mapping import get_mapping

certificate = 'src/config/rds-combined-ca-bundle.pem'

class SqlTester(unittest.TestCase):
    id_ = ''
    url = ''
    db = ''
    table = ''
    test_N = 1000
    table_map = {}
    primary_key = ''
    tz_info = pytz.timezone("Asia/Kolkata")
    N = 1
    
    def get_last_run_cron_job(self):
        client_encr = MongoClient('mongodb://manish:ACVVCH7t7rqd8kB8@cohortx.cluster-cbo3ijdmzhje.ap-south-1.docdb.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&retryWrites=false', tlsCAFile=certificate)
        db_encr = client_encr['dms_migration_updates']
        collection_encr = db_encr['dms_migration_info']
        curs = collection_encr.find({'last_run_cron_job_for_id': self.id_})
        curs = list(curs)
        return curs[0]['timing']

    def last_migrated_record(self):
        client_encr = MongoClient('mongodb://manish:ACVVCH7t7rqd8kB8@cohortx.cluster-cbo3ijdmzhje.ap-south-1.docdb.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&retryWrites=false', tlsCAFile=certificate)
        db_encr = client_encr['dms_migration_updates']
        collection_encr = db_encr['dms_migration_info']
        curs = collection_encr.find({'last_migrated_record_for_id': self.id_})
        curs = list(curs)
        last_record_migrated = curs[0]['record_id']
        if isinstance(last_record_migrated, datetime.datetime):
            last_record_migrated = pytz.utc.localize(last_record_migrated)
        return last_record_migrated

    def abc_test_count(self):
        if(self.table_map['mode'] != 'dumping'):
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
                print(athena_count, N)
                print(confidence(N))
                print(confidence(N) * N)
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
            try:
                athena_key = key.lower()
                if record[key] and not key.startswith('parquet_format'):
                    if column_dtypes[key].startswith('timestamp') or column_dtypes[key].startswith('date'):
                        if record[key] is not pd.NaT and athena_record[athena_key] is not pd.NaT:
                            athena_record[athena_key] = int((pytz.utc.localize(athena_record[athena_key])).timestamp())
                            record[key] = int((record[key]).timestamp())
                        else:
                            athena_record[athena_key] = None
                            record[key] = None
                    elif column_dtypes[key].startswith('double') or column_dtypes[key].startswith('float') or column_dtypes[key].startswith('real') or column_dtypes[key].startswith('decimal') or column_dtypes[key].startswith('numeric'):
                        if numpy.isnan(record[key]) or not record[key]:
                            athena_record[athena_key] = str(athena_record[athena_key])
                            record[key] = str(record[key])
                        else:
                            athena_record[athena_key] = None
                            record[key] = None

                    assert record[key] == athena_record[athena_key]
            except Exception as e:
                print(key)
                print(record[self.primary_key])
                print(record[key])
                print(athena_record[athena_key])
                raise
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
                    df[parq_col + "_year"] = temp.dt.year.astype('float64', copy=False).astype(str)
                    df[parq_col + "_month"] = temp.dt.month.astype('float64', copy=False).astype(str)
                    df[parq_col + "_day"] = temp.dt.day.astype('float64', copy=False).astype(str)
                elif(col_form == 'str'):
                    self.partition_for_parquet.extend([parq_col])
                    df[parq_col] = df[col].astype(str)
                elif(col_form == 'int'):
                    self.partition_for_parquet.extend([parq_col])
                    df[parq_col] = df[col].astype(int)
        return df


    def test_pgsql(self):
        if(self.table_map['mode'] != 'dumping'):
            conn = psycopg2.connect(
                host = self.db['source']['url'],
                database = self.db['source']['db_name'],
                user = self.db['source']['username'],
                password = self.db['source']['password']
            )
            column_dtypes = self.get_column_dtypes(conn=conn, curr_table_name=self.table)
            last_migrated_record = self.last_migrated_record()
            if (self.primary_key == ''):
                return
            
            lim = self.N * self.test_N
            sql_stmt = "SELECT * FROM {0} ORDER BY RANDOM() LIMIT {1}".format(self.table, lim)
            print(sql_stmt)
            data_df = pd.DataFrame({})
            with conn.cursor('test-cursor-name', scrollable=True) as curs:
                curs.execute(sql_stmt)
                _ = curs.fetchone()
                columns = [desc[0] for desc in curs.description]
                curs.scroll(-1)
                for _ in range(self.N):
                    ret = curs.fetchmany(self.test_N)    
                    if not ret:
                        break
                    data_df = pd.DataFrame(ret, columns = columns)
                    
                    ## Adding partitions and a primary key "unique_migration_record_id" for every record
                    self.add_partitions(data_df)
                    self.primary_key = self.primary_key.lower()
                    data_df['unique_migration_record_id'] = data_df[self.primary_key]
                    column_dtypes['unique_migration_record_id'] = 'str'
                    convert_to_dtype(data_df, column_dtypes)
                    athena_table = str(self.table).replace('.', '_').replace('-', '_')
                    data_df = data_df[data_df[self.primary_key] <= last_migrated_record]
                    prev_time = pytz.utc.localize(self.get_last_run_cron_job())
                    if(data_df.shape[0]):
                        if('bookmark' in self.table_map.keys() and self.table_map['bookmark']):
                            data_df = data_df[data_df[self.table_map['bookmark']].apply(lambda x: convert_to_datetime(x=x)) <=  prev_time]
                        
                        str_id = ""
                        for _, row in data_df.iterrows():
                            str_id += "\'" + str(row['unique_migration_record_id']) + "\',"
                        query = 'SELECT * FROM ' + athena_table + ' WHERE unique_migration_record_id in (' + str(str_id[:-1]) + ');'
                        database = "sql" + "_" + self.db['source']['db_name'].replace('.', '_').replace('-', '_')
                        df = wr.athena.read_sql_query(sql = query, database = database)
                        
                        for _, row in data_df.iterrows():
                            athena_record = df.loc[df['unique_migration_record_id'] == row['unique_migration_record_id']].to_dict(orient='records')
                            assert self.check_match(row, athena_record[0], column_dtypes)
                    print("tested", data_df.shape[0], "records")


if __name__ == "__main__":
    N = 200
    id = ''
    if(len(sys.argv) > 1):
        id = sys.argv.pop()
    mapping = get_mapping(id)
    if(mapping['source']['source_type'] == 'sql'):
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
    