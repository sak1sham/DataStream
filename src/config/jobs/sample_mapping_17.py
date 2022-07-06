import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "source": {
        "source_type": "pgsql",
        "db_name": "database-name",
        "password": os.getenv('DB_PASSWORD'),
        "url": os.getenv('SOURCE_DB_URL'),
        "username": os.getenv('DB_USERNAME')
    },
    "destination": {
        "s3": {
            "destination_type": "s3",
            "s3_bucket_name": "s3-bucket-name"
        }
    },
    "tables": [
        {
            "batch_size": 100000,
            "cron": "self-managed",
            "mode": "dumping",
            "partition_col": "migration_snapshot_date",
            "partition_col_format": "datetime",
            "table_name": "audit_logs",
        }
    ]
}