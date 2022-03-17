import pandas as pd
import json
import datetime
from typing import NewType, Any, Dict
from kafka import KafkaConsumer

from helper.util import *
from helper.logger import logger
from helper.exceptions import *
from dst.main import DMS_exporter

datetype = NewType("datetype", datetime.datetime)
dftype = NewType("dftype", pd.DataFrame)

kafka_group = 'dms_kafka'

s3_bucket_destination = 's3://learning-migrationservice/Kafka/'
s3_athena_database = 'kafka'
s3_athena_database_table = 'kafka'

class KafkaMigrate:
    def __init__(self, db: Dict[str, Any] = None, curr_mapping: Dict[str, Any] = None, tz_str: str = 'Asia/Kolkata') -> None:
        if (not db or not curr_mapping):
            raise MissingData("db or curr_mapping can not be None.")
        self.db = db
        self.curr_mapping = curr_mapping
        self.tz_info = pytz.timezone(tz_str)
        self.primary_key = 0

    def inform(self, message: str = None) -> None:
        logger.inform(self.curr_mapping['unique_id'] + ": " + message)

    def warn(self, message: str = None) -> None:
        logger.warn(self.curr_mapping['unique_id'] + ": " + message)

    def err(self, error: Any = None) -> None:
        logger.err(error)

    def preprocess(self) -> None:
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
                self.warn("Unable to find partition_col. Continuing without partitioning.")
        self.curr_mapping['fields']['dms_pkey'] = 'int'
        self.saver = DMS_exporter(db = self.db, uid = self.curr_mapping['unique_id'], partition = self.partition_for_parquet)
        self.athena_dtypes = get_athena_dtypes(self.curr_mapping['fields'])

    def add_partitions(self, df: dftype) -> dftype:
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


    def process_table(self, df: dftype = pd.DataFrame({})) -> dftype:
        end = self.primary_key + len(df)
        df.insert(0, 'dms_pkey', range(self.primary_key, end))
        self.primary_key = end
        df = self.add_partitions(df)
        print(df)
        df = convert_to_dtype(df, self.curr_mapping['fields'])
        processed_data = {'name': self.curr_mapping['topic_name'], 'df_insert': df, 'df_update': pd.DataFrame({}), 'dtypes': self.athena_dtypes}
        return processed_data

    def save_data(self, processed_data: dftype = None) -> None:
        if(not processed_data):
            return
        else:
            if('name' not in processed_data.keys()):
                processed_data['name'] = self.curr_mapping['collection_name']
            if('df_insert' not in processed_data.keys()):
                processed_data['df_insert'] = pd.DataFrame({})
            if('df_update' not in processed_data.keys()):
                processed_data['df_update'] = pd.DataFrame({})
            primary_keys = ['dms_pkey']
            self.saver.save(processed_data = processed_data, primary_keys = primary_keys)


    def value_deserializer(self, x):
        return json.loads(x.decode('utf-8'))

    
    def process(self) -> None:
        try:
            consumer = KafkaConsumer(self.curr_mapping['topic_name'], bootstrap_servers = [self.db['source']['kafka_server']], enable_auto_commit = True, group_id = 'dms_kafka_group', value_deserializer = self.value_deserializer)
            self.inform('Started consuming messages.')
            self.preprocess()
            self.inform("Preprocessing done.")
            for message in consumer:
                try:
                    self.inform("Recieved data")
                    df = pd.DataFrame(message.value)
                    print(df)
                    processed_data = self.process_table(df=df)
                    print(processed_data)
                    self.inform('Processed data')
                    self.save_data(processed_data=processed_data)
                    print("Data saved")
                except Exception as e:
                    print(e)
                    print('Consumer exception')
                    continue
        except Exception as e:
            print("Got some error in Kafka Consumer.")

'''
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test_2
kafka-console-producer --broker-list localhost:9092 --topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic test1 --from-beginning
[{"Name": "John", "Marks": 80, "Class": 12},{"Name": "John II", "Marks": 90, "Class": 10}]
[{"msg": "hi"}, {"msg": "hey"}]
'''