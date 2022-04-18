import traceback
import pandas as pd
import json
import datetime
from typing import NewType, Any, Dict
from kafka import KafkaConsumer

from config.migration_mapping import get_kafka_mapping_functions
from helper.util import *
from helper.logger import logger
from helper.exceptions import *
from dst.main import DMS_exporter

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
        self.batch_size = 1000

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
                                value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        else:
            return KafkaConsumer(topic,
                                bootstrap_servers=kafka_server,
                                enable_auto_commit=enable_auto_commit,
                                group_id=kafka_group,
                                value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    def inform(self, message: str = None, save: bool = False) -> None:
        logger.inform(job_id = self.curr_mapping['unique_id'], s=(self.curr_mapping['unique_id'] + ": " + message), save=save)

    def warn(self, message: str = None) -> None:
        logger.warn(job_id = self.curr_mapping['unique_id'], s=(self.curr_mapping['unique_id'] + ": " + message))

    def err(self, error: Any = None) -> None:
        logger.err(job_id= self.curr_mapping['unique_id'], s=error)

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
        if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):
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
                    parq_col = "parquet_format_" + col
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
                        self.partition_for_parquet.extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                        self.curr_mapping['fields'][parq_col + "_year"] = 'int'
                        self.curr_mapping['fields'][parq_col + "_month"] = 'int'
                        self.curr_mapping['fields'][parq_col + "_day"] = 'int'
                    else:
                        raise UnrecognizedFormat(str(col_form) + ". Partition_col_format can be int, float, str or datetime")
            else:
                self.warn(message=("Unable to find partition_col. Continuing without partitioning."))
        self.curr_mapping['fields']['dms_pkey'] = 'int'
        self.saver = DMS_exporter(db = self.db, uid = self.curr_mapping['unique_id'], partition = self.partition_for_parquet)
        self.athena_dtypes = get_athena_dtypes(self.curr_mapping['fields'])

    def add_partitions(self, df: dftype) -> dftype:
        '''
            If partitions are specified in mapping, add partition fields to the documents.
        '''
        for i in range(len(self.curr_mapping['partition_col'])):
            col = self.curr_mapping['partition_col'][i]
            col_form = self.curr_mapping['partition_col_format'][i]
            parq_col = "parquet_format_" + col
            if(col_form == 'datetime'):
                temp = pd.to_datetime(df[col], errors='coerce', utc=True).apply(lambda x: pd.Timestamp(x))
                df[parq_col + "_year"] = temp.dt.year
                df[parq_col + "_month"] = temp.dt.month
                df[parq_col + "_day"] = temp.dt.day
            elif(col_form == 'str'):
                df[parq_col] = df[col].astype(str)
            elif(col_form == 'int'):
                df[parq_col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int, copy=False, errors='ignore')
            else:
                raise UnrecognizedFormat(str(col_form) + ". Partition_col_format can be int, str or datetime.") 
        return df


    def process_table(self, df: dftype) -> dftype:
        end = self.primary_key + len(df)
        df.insert(0, 'dms_pkey', range(self.primary_key, end))
        self.primary_key = end
        df = self.add_partitions(df)
        df = convert_to_dtype(df, self.curr_mapping['fields'])
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
                processed_data['name'] = self.curr_mapping['collection_name']
            if('df_insert' not in processed_data.keys()):
                processed_data['df_insert'] = pd.DataFrame({})
            if('df_update' not in processed_data.keys()):
                processed_data['df_update'] = pd.DataFrame({})
            if('dtypes' not in processed_data.keys()):
                processed_data['dtypes'] = get_athena_dtypes(self.curr_mapping['fields'])
            primary_keys = ['dms_pkey']
            self.saver.save(processed_data = processed_data, primary_keys = primary_keys)


    def value_deserializer(self, x):
        return json.loads(x.decode('utf-8'))

    
    def process(self) -> None:
        '''
                This function handles the entire flow of preprocessing, processing and saving data.
            '''
        try:
            '''
                Consumes the data in kafka
            '''
            consumer = self.get_kafka_connection(topic=self.curr_mapping['topic_name'], kafka_group=self.db['source']['consumer_group_id'], kafka_server=self.db['source']['kafka_server'], KafkaUsername=self.db['source']['kafka_username'], KafkaPassword=self.db['source']['kafka_password'], enable_auto_commit=True)
            self.inform(message='Started consuming messages.', save=True)
            self.preprocess()
            self.inform(message="Preprocessing done.", save=True)
            try:
                while(1):
                    recs = consumer.poll(timeout_ms=1000000, max_records=self.batch_size)
                    if(not recs):
                        self.inform('No more records found. Stopping the script')
                        break
                    else:
                        for _, records in recs.items():
                            converted_list = []
                            for message in records:
                                converted_list.append(self.process_dict(message.value))
                            converted_df = pd.DataFrame(converted_list)
                            processed_data = self.process_table(df=converted_df)
                            self.save_data(processed_data=processed_data)
                            self.inform(message="Data saved")
            except Exception as e:
                self.inform(traceback.format_exc())
                self.err(error=('Consumer exception', e))
        except Exception as e:
            self.err(error=("Got some error in Kafka Consumer.", e))

'''
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test_2
kafka-console-producer --broker-list localhost:9092 --topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic test1 --from-beginning

[{"Name": "John", "Marks": 80, "Class": 12},{"Name": "John II", "Marks": 90, "Class": 10}]
[{"msg": "hi"}, {"msg": "hey"}]
'''