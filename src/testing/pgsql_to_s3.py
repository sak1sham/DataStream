import psycopg2
import awswrangler as wr
import datetime
from typing import NewType
import pytz
from pymongo import MongoClient
datetype = NewType("datetype", datetime.datetime)
import sys
import numpy
import time 
import traceback
from typing import Dict, Any

from test_util import *
from migration_mapping import get_mapping
from slack_notify import send_message
from testing_logger import logger

import os
from dotenv import load_dotenv

load_dotenv()

certificate = 'config/rds-combined-ca-bundle.pem'

class SqlTester():
    def __init__(self, id_: str = '', url: str = '', db: Dict = {}, table: Dict = {}, test_N: int = 1000, table_map: Dict = {}, primary_key: str = '', tz_info: Any = pytz.timezone("Asia/Kolkata"), N: int = 1):
        self.id_ = id_
        self.url = url
        self.db = db
        self.table = table
        self.test_N = test_N
        self.table_map = table_map
        self.primary_key = primary_key
        self.tz_info = tz_info
        self.N = N
        self.count = 0

    def get_last_run_cron_job(self):
        client_encr = MongoClient(os.getenv('ENCR_MONGO_URL'), tlsCAFile=certificate)
        db_encr = client_encr[os.getenv('DB_NAME')]
        collection_encr = db_encr[os.getenv('COLLECTION_NAME')]
        curs = collection_encr.find({'last_run_cron_job_for_id': self.id_})
        curs = list(curs)
        return curs[0]['timing']

    def last_migrated_record(self):
        client_encr = MongoClient(os.getenv('ENCR_MONGO_URL'), tlsCAFile=certificate)
        db_encr = client_encr[os.getenv('DB_NAME')]
        collection_encr = db_encr[os.getenv('COLLECTION_NAME')]
        curs = collection_encr.find({'last_migrated_record_for_id': self.id_})
        curs = list(curs)
        last_record_migrated = curs[0]['record_id']
        if isinstance(last_record_migrated, datetime.datetime):
            last_record_migrated = pytz.utc.localize(last_record_migrated)
        return last_record_migrated

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
                logger.err(key)
                logger.err(record[self.primary_key])
                logger.err("Source: " + str(record[key]))
                logger.err("Destination: " + str(athena_record[athena_key]))
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
                    df[parq_col] = df[col].fillna(0).astype(int)
        return df

    def test_pgsql(self) -> int:
        self.count = 0
        prev_time = pytz.utc.localize(self.get_last_run_cron_job())
        logger.inform("Previous run cron time: " + str(prev_time))
        compare_datetime = prev_time
        if('buffer_updation_lag' in self.table_map.keys() and self.table_map['buffer_updation_lag']):
            days = 0
            hours = 0
            minutes = 0
            if('days' in self.table_map['buffer_updation_lag'].keys() and self.table_map['buffer_updation_lag']['days']):
                days = self.table_map['buffer_updation_lag']['days']
            if('hours' in self.table_map['buffer_updation_lag'].keys() and self.table_map['buffer_updation_lag']['hours']):
                hours = self.table_map['buffer_updation_lag']['hours']
            if('minutes' in self.table_map['buffer_updation_lag'].keys() and self.table_map['buffer_updation_lag']['minutes']):
                minutes = self.table_map['buffer_updation_lag']['minutes']
            compare_datetime = compare_datetime - datetime.timedelta(days=days, hours=hours, minutes=minutes)
        logger.inform("Testing records before: " + str(compare_datetime))
        
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
            logger.inform(sql_stmt)
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
                    if(data_df.shape[0]):
                        if('bookmark' in self.table_map.keys() and self.table_map['bookmark']):
                            logger.inform("Before filter: " + str(data_df[self.table_map['bookmark']].max()))
                            data_df_NaT = data_df[data_df[self.table_map['bookmark']].isnull()]
                            data_df_old = data_df[data_df[self.table_map['bookmark']].apply(lambda x: convert_to_datetime(x=x, tz_ = pytz.timezone('Asia/Kolkata'))) <  compare_datetime]
                            data_df = pd.concat([data_df_NaT, data_df_old])
                            logger.inform("After filter: " + str(data_df[self.table_map['bookmark']].max()))
                        str_id = ""
                        for _, row in data_df.iterrows():
                            str_id += "\'" + str(row['unique_migration_record_id']) + "\',"
                        query = 'SELECT * FROM ' + athena_table + ' WHERE unique_migration_record_id in (' + str(str_id[:-1]) + ');'
                        database = "pgsql" + "_" + self.db['source']['db_name'].replace('.', '_').replace('-', '_')
                        df = wr.athena.read_sql_query(pgsql = query, database = database)
                        
                        for _, row in data_df.iterrows():
                            athena_record = df.loc[df['unique_migration_record_id'] == row['unique_migration_record_id']].to_dict(orient='records')
                            try:
                                assert self.check_match(row, athena_record[0], column_dtypes)
                            except Exception as e:
                                logger.err(row[self.primary_key])
                                logger.err(traceback.format_exc())
                                self.count += 1
                    logger.inform("Tested {0} records.".format(data_df.shape[0]))
        return self.count


if __name__ == "__main__":
    try:
        N = 1000
        records_per_batch = 1000
        id = ''
        if(len(sys.argv) > 1):
            id = sys.argv.pop()
        mapping = get_mapping(id)
        if('username' not in mapping['source'].keys()):
            mapping['source']['username'] = ''
            mapping['source']['password'] = ''
        if(mapping['source']['source_type'] == 'pgsql'):
            if('tables' not in mapping.keys()):
                mapping['tables'] = []
            for table in mapping['tables']:
                start = time.time()
                obj = SqlTester(N=N, url=mapping['source']['url'], db = mapping, id_ = id + "_DMS_" + table['table_name'], table = table['table_name'], table_map = table, primary_key = table['primary_key'], test_N=records_per_batch)
                logger.inform("Testing " + str(table['table_name']))
                mismatch = obj.test_pgsql()
                end = time.time()
                time_taken = str(datetime.timedelta(seconds=int(end-start)))
                if('notify' in settings.keys() and settings['notify']):
                    msg = "Testing completed for *{0}* from database *{1}* ({2}) with desination {3}.\nTested {4} random records\nTotal time taken {5}\nFound {6} mismatches".format(table['table_name'], mapping['source']['db_name'], mapping['source']['source_type'], mapping['destination']['destination_type'], N*records_per_batch, str(time_taken), mismatch)
                    if(mismatch > 0):
                        msg = msg + " <!channel>"
                    try:
                        slack_token = settings['slack_notif']['slack_token']
                        channel = mapping['slack_channel'] if 'slack_channel' in mapping and mapping['slack_channel'] else settings['slack_notif']['channel']
                        send_message(msg = msg, channel = channel, slack_token = slack_token)
                        logger.inform("Testing notification sent successfully.")
                    except Exception as e:
                        logger.err(traceback.format_exc())
                        logger.err("Unable to connect to slack and send the notification.")
    except Exception as e:
        if('notify' in settings.keys() and settings['notify']):
            msg = "Testing failed for *{0}* from database *{1}* ({2}) with desination {3} with following exception:\n```{4}```".format(table['table_name'], mapping['source']['db_name'], mapping['source']['source_type'], mapping['destination']['destination_type'], traceback.format_exc())
            try:
                slack_token = settings['slack_notif']['slack_token']
                channel = mapping['slack_channel'] if 'slack_channel' in mapping and mapping['slack_channel'] else settings['slack_notif']['channel']
                send_message(msg = msg, channel = channel, slack_token = slack_token)
                logger.inform("Testing notification sent successfully.")
            except Exception as e:
                logger.err(traceback.format_exc())
                logger.err("Unable to connect to slack and send the notification.")