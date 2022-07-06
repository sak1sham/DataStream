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
            "batch_size": 10000,
            "bookmark": "updated_at_for_pipeline",
            "buffer_updation_lag": {
                "hours": 2
            },
            "cron": "self-managed",
            "grace_updation_lag": {
                "days": 1
            },
            "improper_bookmarks": False,
            "mode": "syncing",
            "partition_col": "created_at",
            "partition_col_format": "datetime",
            "primary_key": "po_id",
            "primary_key_datatype": "int",
            "table_name": "purchase_orders",
        }
    ]
}