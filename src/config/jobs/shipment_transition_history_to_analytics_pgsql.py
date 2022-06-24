import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "destination": {
        'ec2_1': {
            "db_name": "cmdb",
            "schema": "public",
            "password": os.getenv('DB_PASSWORD'),
            "url": "dms2.citymall.dev",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
        },
        'ec2_2': {
            "db_name": "cmdb",
            "schema": "public",
            "password": os.getenv('DB_PASSWORD'),
            "url": "dms1.citymall.dev",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
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
            "batch_size": 100000,
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
            "primary_key": "id",
            "primary_key_datatype": "int",
            "table_name": "shipment_transition_history",
        }
    ]
}