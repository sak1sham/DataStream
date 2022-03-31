import awswrangler as wr

from dotenv import load_dotenv

load_dotenv()

path = 's3://database-migration-service-prod/mongo/support-service/support_tickets/parquet_format__id_year=2022/parquet_format__id_month=3/parquet_format__id_day=24/657f737c6e2b41b0b9b679a4c38f7462.snappy.parquet'
df = wr.s3.read_parquet(path = [path])
print(df.shape)