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
        "db_name": "database-name",
        "password": os.getenv('DB_PASSWORD'),
        "source_type": "pgsql",
        "url": os.getenv('SOURCE_DB_URL'),
        "username": os.getenv('DB_USERNAME')
    },
    "tables": [
        {
            "batch_size": 10000,
            "cron": "self-managed",
            "mode": "dumping",
            "partition_col": "created_at",
            "partition_col_format": "datetime",
            "table_name": "product_inventory_transactions",
        }
    ]
}