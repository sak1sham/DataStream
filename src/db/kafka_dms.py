import traceback
import pandas as pd
import json
import datetime
from typing import NewType, Any, Dict
from kafka import KafkaConsumer
import redis
import time
import uuid
from retrying import retry

from config.migration_mapping import get_kafka_mapping_functions
from helper.util import *
from helper.logger import logger
from helper.exceptions import *
from dst.main import DMS_exporter
from notifications.slack_notify import send_message

datetype = NewType("datetype", datetime.datetime)
dftype = NewType("dftype", pd.DataFrame)

class KafkaMigrate:
    def __init__(self, db: Dict[str, Any] = None, curr_mapping: Dict[str, Any] = None, tz_str: str = 'Asia/Kolkata') -> None:
        if (not db or not curr_mapping):
            raise MissingData("db or curr_mapping can not be None.")
        self.db = db
        self.curr_mapping = curr_mapping
        self.tz_info = pytz.timezone(tz_str)
        self.primary_key = 0
        self.get_table_name, self.process_dict = get_kafka_mapping_functions(self.db['id'])
        self.table_name = self.curr_mapping['topic_name']
        self.batch_size = 10
        redis_url = db['redis']['url']
        redis_password = db['redis']['password']
        self.redis_db = redis.StrictRedis.from_url(url = redis_url, password=redis_password, decode_responses=True)
        self.redis_key = self.curr_mapping['unique_id']


    def get_kafka_connection(self, topic, kafka_group, kafka_server, KafkaPassword, KafkaUsername, enable_auto_commit = True):
        if "aws" in kafka_server:
            return KafkaConsumer(topic,
                                bootstrap_servers=kafka_server,
                                security_protocol='SASL_SSL',
                                sasl_mechanism='SCRAM-SHA-512',
                                sasl_plain_username=KafkaUsername,
                                sasl_plain_password=KafkaPassword,
                                enable_auto_commit=enable_auto_commit,
                                #  auto_offset_reset='earliest',
                                group_id=kafka_group,
                                value_deserializer=lambda m: m)
        else:
            return KafkaConsumer(topic,
                                bootstrap_servers=kafka_server,
                                enable_auto_commit=enable_auto_commit,
                                group_id=kafka_group,
                                value_deserializer=lambda m: m)

    def inform(self, message: str = None) -> None:
        logger.inform(s = f"{self.curr_mapping['unique_id']}: {message}")

    def warn(self, message: str = None) -> None:
        logger.warn(s = f"{self.curr_mapping['unique_id']}: {message}")

    def err(self, error: Any = None) -> None:
        logger.err(s = error)

    def preprocess(self) -> None:
        '''
            This function handles all the preprocessing steps.
                1. If partitions need to be made, store separate partition fields and their datatypes in the job-mapping.
                2. If data is to be dumped, add a field 'migration_snapshot_date' in job-mapping with format as datetime
                3. Create a saver object to save data at destination.
        '''
        if('fields' not in self.curr_mapping.keys()):
            self.curr_mapping['fields'] = {}
        
        self.partition_for_parquet = []
        if('partition_col' in self.curr_mapping.keys() and self.curr_mapping['partition_col']):
            if(isinstance(self.curr_mapping['partition_col'], str)):
                self.curr_mapping['partition_col'] = [self.curr_mapping['partition_col']]
            
            if('partition_col_format' not in self.curr_mapping.keys()):
                self.curr_mapping['partition_col_format'] = ['str']
            if(isinstance(self.curr_mapping['partition_col_format'], str)):
                self.curr_mapping['partition_col_format'] = [self.curr_mapping['partition_col_format']]
            while(len(self.curr_mapping['partition_col']) > len(self.curr_mapping['partition_col_format'])):
                self.curr_mapping['partition_col_format'].append('str')
            
            for i in range(len(self.curr_mapping['partition_col'])):
                col = self.curr_mapping['partition_col'][i]
                col_form = self.curr_mapping['partition_col_format'][i]
                parq_col = f"parquet_format_{col}"
                if(col_form == 'str'):
                    self.partition_for_parquet.extend([parq_col])
                    self.curr_mapping['fields'][parq_col] = 'str'
                elif(col_form == 'int'):
                    self.partition_for_parquet.extend([parq_col])
                    self.curr_mapping['fields'][parq_col] = 'int'
                elif(col_form == 'float'):
                    self.partition_for_parquet.extend([parq_col])
                    self.curr_mapping['fields'][parq_col] = 'float'
                elif(col_form == 'datetime'):
                    self.partition_for_parquet.extend([f"{parq_col}_year", f"{parq_col}_month", f"{parq_col}_day"])
                    self.curr_mapping['fields'][f"{parq_col}_year"] = 'int'
                    self.curr_mapping['fields'][f"{parq_col}_month"] = 'int'
                    self.curr_mapping['fields'][f"{parq_col}_day"] = 'int'
                else:
                    raise UnrecognizedFormat(f"{str(col_form)}. Partition_col_format can be int, float, str or datetime")
        else:
            self.warn(message="Continuing without partitioning data.")
        self.curr_mapping['fields']['dms_pkey'] = 'str'

        self.saver = DMS_exporter(db = self.db, uid = self.curr_mapping['unique_id'], partition = self.partition_for_parquet)

        self.athena_dtypes = get_athena_dtypes(self.curr_mapping['fields'])
        if('col_rename' in self.curr_mapping.keys() and self.curr_mapping['col_rename']):
            for key, val in self.curr_mapping['col_rename'].items():
                self.athena_dtypes[val] = self.athena_dtypes[key]
                self.athena_dtypes.pop(key)

    def add_partitions(self, df: dftype) -> dftype:
        '''
            If partitions are specified in mapping, add partition fields to the documents.
        '''
        for i in range(len(self.curr_mapping['partition_col'])):
            col = self.curr_mapping['partition_col'][i]
            col_form = self.curr_mapping['partition_col_format'][i]
            parq_col = f"parquet_format_{col}"
            if(col_form == 'datetime'):
                temp = pd.to_datetime(df[col], errors='coerce', utc=True).apply(lambda x: pd.Timestamp(x))
                df[f"{parq_col}_year"] = temp.dt.year.astype('float64', copy=False).astype(str)
                df[f"{parq_col}_month"] = temp.dt.month.astype('float64', copy=False).astype(str)
                df[f"{parq_col}_day"] = temp.dt.day.astype('float64', copy=False).astype(str)
            elif(col_form == 'str'):
                df[parq_col] = df[col].astype(str)
            elif(col_form == 'int'):
                df[parq_col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int, copy=False, errors='ignore')
            else:
                raise UnrecognizedFormat(f"{str(col_form)}. Partition_col_format can be int, str or datetime.") 
        return df


    def process_table(self, df: dftype) -> dftype:

        df['dms_pkey'] = [uuid.uuid4() for _ in range(len(df.index))]
        df = self.add_partitions(df)
        df = convert_to_dtype(df, self.curr_mapping['fields'])
        if('col_rename' in self.curr_mapping.keys() and self.curr_mapping['col_rename']):
            df.rename(columns=self.curr_mapping['col_rename'], inplace=True)
        processed_data = {'name': self.table_name, 'df_insert': df, 'df_update': pd.DataFrame({}), 'dtypes': self.athena_dtypes}
        return processed_data

    def save_data(self, processed_data: dftype = None) -> None:
        '''
            This function saves the processed data into destination
        '''
        if(not processed_data):
            return
        else:
            if('name' not in processed_data.keys()):
                processed_data['name'] = self.table_name
            if('df_insert' not in processed_data.keys()):
                processed_data['df_insert'] = pd.DataFrame({})
            if('df_update' not in processed_data.keys()):
                processed_data['df_update'] = pd.DataFrame({})
            if('dtypes' not in processed_data.keys()):
                processed_data['dtypes'] = self.athena_dtypes
            primary_keys = ['dms_pkey']
            self.saver.save(processed_data = processed_data, primary_keys = primary_keys)


    @retry(wait_random_min=10000, wait_random_max=20000, stop_max_attempt_number=10)
    def redis_push(self, message):
        try:
            self.redis_db.rpush(self.redis_key, message.value)
        except Exception as e:
            logger.err(traceback.format_exc())
            raise

    @retry(wait_random_min=10000, wait_random_max=20000, stop_max_attempt_number=10)
    def redis_pop(self):
        try:
            data = self.redis_db.lpop(self.redis_key, count=self.batch_size)
            return data
        except Exception as e:
            logger.err(traceback.format_exc())
            raise

    def value_deserializer(self, x):
        return json.loads(x.decode('utf-8'))

    
    def process(self) -> None:
        '''
            This function handles the entire flow of preprocessing, processing and saving data.
        '''

        '''
            Consumes the data in kafka
        '''
        consumer = self.get_kafka_connection(topic=self.curr_mapping['topic_name'], kafka_group=self.db['source']['consumer_group_id'], kafka_server=self.db['source']['kafka_server'], KafkaUsername=self.db['source']['kafka_username'], KafkaPassword=self.db['source']['kafka_password'], enable_auto_commit=True)
        self.inform(message='Started consuming messages.')
        self.preprocess()
        self.inform(message="Preprocessing done.")
        batch_start_time = time.time()
        total_redis_insertion_time = 0
        while (1):
            try:
                for message in consumer:
                    start_time = time.time()
                    self.redis_push(message)    
                    total_redis_insertion_time += time.time() - start_time
                    self.inform(f"Reached {self.redis_db.llen(self.redis_key)}/{self.batch_size}")
                    if(self.redis_db.llen(self.redis_key) >= self.batch_size):
                        self.inform(f"Time taken in (consuming + redis insertions) of {self.batch_size} records: {time.time() - batch_start_time} seconds")
                        self.inform(f"Time taken in (redis insertions) of {self.batch_size} records: {total_redis_insertion_time} seconds")
                        start_time = time.time()
                        list_records = self.redis_pop()

                        segregated_recs = {}
                        for val in list_records:
                            val = json.loads(val)
                            val_table_name = self.get_table_name(val)
                            if(val_table_name not in segregated_recs.keys()):
                                segregated_recs[val_table_name] = [self.process_dict(val)]
                            else:
                                segregated_recs[val_table_name].append(self.process_dict(val))
                        
                        for table_name, recs in segregated_recs.items():
                            converted_df = pd.DataFrame(recs)
                            processed_data = self.process_table(df=converted_df)
                            processed_data['name'] = table_name
                            self.save_data(processed_data=processed_data)
                            self.inform(message="Data saved")

                        self.inform(f"Time taken to migrate a batch to s3: {time.time() - start_time} seconds")
                        batch_start_time = time.time()
                        total_redis_insertion_time = 0
            except Exception as e:
                msg = f"Caught some exception: {e}"
                slack_token = settings['slack_notif']['slack_token']
                slack_channel = settings['slack_notif']['channel']
                send_message(msg = msg, channel = slack_channel, slack_token = slack_token)
                logger.err(traceback.format_exc())