import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "destination": {
        'ec2_1': {
            "db_name": "cmdb",
            "password": os.getenv('DB_PASSWORD'),
            "url": "15.206.171.84",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
            "schema": "public"
        },
        'ec2_2': {
            "db_name": "cmdb",
            "password": os.getenv('DB_PASSWORD'),
            "url": "13.233.225.181",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
            "schema": "public"
        }
    },
    "source": {
        "db_name": "cmdb",
        "password": os.getenv('DB_PASSWORD'),
        "source_type": "pgsql",
        "url": os.getenv('CMDB_URL'),
        "username": os.getenv('DB_USERNAME')
    },
    "tables": [
        {
            "batch_size": 10000,
            'strict': True,
            "bookmark": "updated_at_for_pipeline",
            "buffer_updation_lag": {
                "hours": 2
            },
            "cron": "self-managed",
            "grace_updation_lag": {
                "days": 1
            },
            "improper_bookmarks": False,
            "mode": "mirroring",
            "partition_col": "created_at",
            "partition_col_format": "datetime",
            "primary_key": "id",
            "primary_key_datatype": "int",
            "table_name": "batch_table_summary",
        }
    ]
}