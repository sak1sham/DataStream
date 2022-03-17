import pandas as pd
import awswrangler as wr
from dotenv import load_dotenv
import os
import json
import datetime
from typing import NewType
from kafka import KafkaConsumer
from os.path import join, dirname

from helper.util import *
from helper.logger import logger


dotenv_path = join(dirname(__file__), '../impressions.env')
load_dotenv(dotenv_path)

datetype = NewType("datetype", datetime.datetime)
dftype = NewType("dftype", pd.DataFrame)

topic = os.environ.get('KAFKA_IMPRESSION_S3_TOPIC')
kafka_server = os.environ.get('KAFKA_BOOTSTRAP_SERVER')
kafka_group = os.environ.get('KAFKA_GROUP')
s3_bucket_destination = os.environ.get('s3_bucket_destination')
s3_athena_database=os.environ.get('s3_athena_database')
s3_athena_database_table=os.environ.get('s3_athena_database_table')

class KafkaMigrate:
    def __init__(self) -> None:
        self.curr_mapping = {
            'fields': {
                'Screen_name': 'str',
                'User_id': 'str',
                'Ct_profile_id': 'str',
                'Asset_id': 'int',
                'Asset_type': 'str',
                'Asset_parent_id': 'str',
                'Asset_parent_type': 'str',
                'Price': 'int',
                'Mrp': 'int',
                'Action': 'str',
                'App_type': 'str',
                'Date': 'datetime',
                'Entity_type': 'str',
                'Vertical_rank': 'int',
                'Horizontal_rank': 'int',
                'Source': 'str',
                'Is_product_oos': 'bool',
                'Catalogue_name': 'str',
                'Insertion_date': 'datetime',
                'Cms_page_id': 'str',
                'Linked_cms': 'str',
                'Linked_cat': 'str',
                'Linked_subcat': 'str',
            },
            's3_bucket_destination': s3_bucket_destination,
        }

    def process_table(self, df: dftype = pd.DataFrame({})) -> dftype:
        self.athena_dtypes = get_athena_dtypes(self.curr_mapping['fields'])
        temp = df['Insertion_date'].apply(lambda x: convert_to_datetime(x))
        df["Insertion_date_year"] = temp.dt.year
        df["Insertion_date_month"] = temp.dt.month
        df["Insertion_date_day"] = temp.dt.day
        schema = self.curr_mapping['fields']
        df = convert_to_dtype(df, schema)
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
            description='data coming from impression service',
            dataset=True,
            partition_cols=['Insertion_date_year',
                            'Insertion_date_month', 'Insertion_date_day'],
            schema_evolution=True,
        )

def value_deserializer(x):
    return json.loads(x.decode('utf-8'))

if __name__ == "__main__":
    obj = KafkaMigrate()
    consumer = KafkaConsumer(topic, bootstrap_servers = [kafka_server], enable_auto_commit = True, group_id = kafka_group, value_deserializer = value_deserializer)
    logger.inform('Started consuming messages.')
    for message in consumer:
        try:
            df = pd.DataFrame(message.value)
            print(df.columns.tolist())
            processed_data = obj.process_table(df=df)
            obj.save_data(processed_data=processed_data)
            logger.inform("Data saved")
        except Exception as e:
            print(e)
            logger.err('Consumer exception')
            continue
