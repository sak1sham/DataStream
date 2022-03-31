import pyarrow.parquet as pq
import s3fs
s3 = s3fs.S3FileSystem()
from dotenv import load_dotenv
load_dotenv()

path = 's3://database-migration-service-prod/mongo/support-service/support_tickets/parquet_format__id_year=2021/parquet_format__id_month=12/parquet_format__id_day=21/b46384e6b6fa4a9ea6bc16bb372b6f12.snappy.parquet'
df = pq.ParquetDataset(path, filesystem=s3).read_pandas().to_pandas()


# import awswrangler as wr
# df = wr.s3.read_parquet(path = path)
print(df.shape)