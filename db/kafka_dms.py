import pandas as pd
import awswrangler as wr
import json
import datetime
from typing import NewType, Any, Dict
from kafka import KafkaConsumer
import logging
import pytz

from dotenv import load_dotenv
load_dotenv()

#from helper.util import *
#from helper.logger import logger

datetype = NewType("datetype", datetime.datetime)
dftype = NewType("dftype", pd.DataFrame)

topic = 'test_2'
kafka_server = 'localhost:9092'
kafka_group = 'dms_kafka'

s3_bucket_destination = 's3://learning-migrationservice/Kafka/'
s3_athena_database = 'kafka'
s3_athena_database_table = 'kafka'

# util function below 

def convert_to_utc(dt: datetype = None) -> datetype:
    if(dt.tzinfo is None):
        dt = pytz.utc.localize(dt)
    dt = dt.astimezone(pytz.utc)
    return dt


def convert_to_datetime(x: Any = None) -> datetype:
    if(x is None or x == pd.Timestamp(None) or x is pd.NaT):
        return pd.Timestamp(None)
    elif(isinstance(x, datetime.datetime)):
        x = convert_to_utc(dt=x)
        return x
    elif(isinstance(x, int) or isinstance(x, float)):
        x = datetime.datetime.fromtimestamp(x, pytz.utc)
        return pd.to_datetime(x, utc=True)
    else:
        try:
            x = pd.to_datetime(x, utc=True)
            return x
        except Exception as e:
            return pd.Timestamp(None)


def convert_to_dtype(df: dftype, schema: Dict[str, Any]) -> dftype:
    if(df.shape[0]):
        for col in df.columns.tolist():
            if(col in schema.keys()):
                dtype = schema[col].lower()
                if(dtype == 'jsonb' or dtype == 'json'):
                    df[col] = df[col].apply(lambda x: json.dumps(x))
                    df[col] = df[col].astype(str)
                elif(dtype.startswith('timestamp') or dtype.startswith('date')):
                    df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).apply(
                        lambda x: pd.Timestamp(x))
                elif(dtype == 'boolean' or dtype == 'bool'):
                    df[col] = df[col].astype(bool)
                elif(dtype == 'bigint' or dtype == 'integer' or dtype == 'smallint' or dtype == 'bigserial' or dtype == 'smallserial' or dtype.startswith('serial') or dtype.startswith('int')):
                    df[col] = pd.to_numeric(
                        df[col], errors='coerce').fillna(0).astype(int)
                elif(dtype == 'double precision' or dtype.startswith('numeric') or dtype == 'real' or dtype == 'double' or dtype == 'money' or dtype.startswith('decimal') or dtype.startswith('float')):
                    df[col] = pd.to_numeric(
                        df[col], errors='coerce').astype(float)
                else:
                    df[col] = df[col].astype(str)
            else:
                df[col] = df[col].astype(str)
    return df


def get_athena_dtypes(maps: Dict[str, str] = {}) -> Dict[str, str]:
    athena_types = {}
    for key, dtype in maps.items():
        if(dtype == 'str' or dtype == 'string' or dtype == 'jsonb' or dtype == 'json' or dtype == 'cidr' or dtype == 'inet' or dtype == 'macaddr' or dtype == 'uuid' or dtype == 'xml' or 'range' in dtype or 'interval' in dtype):
            athena_types[key] = 'string'
        elif(dtype == 'datetime' or dtype.startswith('timestamp') or dtype.startswith('date')):
            athena_types[key] = 'timestamp'
        elif(dtype == 'bool' or dtype == 'boolean'):
            athena_types[key] = 'boolean'
        elif(dtype == 'int' or dtype == 'bigint' or dtype == 'integer' or dtype == 'smallint' or dtype == 'bigserial' or dtype == 'smallserial' or dtype == 'serial' or dtype.startswith('serial') or dtype.startswith('int')):
            athena_types[key] = 'bigint'
        elif(dtype == 'float' or dtype == 'double precision' or dtype.startswith('numeric') or dtype == 'real' or dtype == 'double' or dtype == 'money' or dtype.startswith('decimal') or dtype.startswith('float')):
            athena_types[key] = 'float'
    return athena_types




class KafkaMigrate:
    def __init__(self) -> None:
        self.curr_mapping = {
            'topic_name': 'test_2',
            'fields': {
                'msg': 'str',
            },
            's3_bucket_destination': s3_bucket_destination,
        }

    def process_table(self, df: dftype = pd.DataFrame({})) -> dftype:
        self.athena_dtypes = get_athena_dtypes(self.curr_mapping['fields'])
        df["msg_parquet"] = df['msg']
        schema = self.curr_mapping['fields']
        df = convert_to_dtype(df, schema)
        {'name': 'kafka'}
        return df

    def save_data(self, processed_data: dftype = None) -> None:
        wr.s3.to_parquet(
            df=processed_data,
            path=self.curr_mapping['s3_bucket_destination'],
            compression='snappy',
            mode='append',
            database=s3_athena_database,
            table=s3_athena_database_table,
            dtype=self.athena_dtypes,
            description='data coming from kafka producer',
            dataset=True,
            partition_cols=['msg_parquet'],
            schema_evolution=True,
        )

def value_deserializer(x):
    return json.loads(x.decode('utf-8'))

if __name__ == "__main__":
    obj = KafkaMigrate()
    consumer = KafkaConsumer(topic, bootstrap_servers = [kafka_server], enable_auto_commit = True, group_id = kafka_group, value_deserializer = value_deserializer)
    print('Started consuming messages.')
    for message in consumer:
        try:
            print(message)
            df = pd.DataFrame(message.value)
            print(df.columns.tolist())
            processed_data = obj.process_table(df=df)
            print('processed data')
            obj.save_data(processed_data=processed_data)
            print("Data saved")
        except Exception as e:
            print(e)
            print('Consumer exception')
            continue


'''
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test_2
kafka-console-producer --broker-list localhost:9092 --topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic test1 --from-beginning
[{"msg": "hi"}, {"msg": "hey"}]
'''