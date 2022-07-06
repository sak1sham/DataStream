import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "destination": {
        "s3": {
            "destination_type": "s3",
            "s3_bucket_name": "s3-bucket-name"
        }
    },
    "source": {
        "db_name": "db-name",
        "password": os.getenv('DB_PASSWORD'),
        "source_type": "pgsql",
        "url": os.getenv('SOURCE_DB_URL'),
        "username": os.getenv('DB_USERNAME')
    },
    "tables": [
        {
            "batch_size": 100000,
            "cron": "self-managed",
            "mode": "logging",
            "partition_col": "timestamp",
            "partition_col_format": "datetime",
            "primary_key": "id",
            "primary_key_datatype": "int",
            "table_name": "rm_events",
        }
    ]
}