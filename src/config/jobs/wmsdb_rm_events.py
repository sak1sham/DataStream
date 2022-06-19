import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "destination": {
        "s3": {
            "destination_type": "s3",
            "s3_bucket_name": "database-migration-service-prod"
        }
    },
    "source": {
        "db_name": "wmsdb",
        "password": os.getenv('DB_PASSWORD'),
        "source_type": "pgsql",
        "url": os.getenv('CMDB_URL'),
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