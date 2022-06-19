import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "destination": {
        'ec2_1': {
            "db_name": "wmsdb",
            "schema": "public",
            "password": os.getenv('DB_PASSWORD'),
            "url": "15.206.171.84",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
        },
        'ec2_2': {
            "db_name": "wmsdb",
            "schema": "public",
            "password": os.getenv('DB_PASSWORD'),
            "url": "13.233.225.181",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
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
            'strict': True,
            "cron": "self-managed",
            "mode": "dumping",
            "partition_col": "created_at",
            "partition_col_format": "datetime",
            "table_name": "rm_skus_area_wise_with_mrp_snapshot",
        }
    ]
}