import awswrangler as wr

import pandas as pd

from dotenv import load_dotenv
load_dotenv() 

df = wr.s3.read_parquet(
    path = ['s3://database-migration-service-prod/mongo/support-service/support_tickets_rating/parquet_format__id_year=2021/parquet_format__id_month=12/parquet_format__id_day=10/parquet_format__id_hour=0/128c253e6aee44499f86034b4b2ded3b.snappy.parquet'],
)

print(df.head())

print(df.columns.tolist())
print(df.dtypes)