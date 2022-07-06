import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "destination": {
        'destination_1': {
            "db_name": "database-name",
            "schema": "wms",
            "password": os.getenv('DB_PASSWORD'),
            "url": "destination.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
        },
        'destination_2': {
            "db_name": "database-name",
            "schema": "wms",
            "password": os.getenv('DB_PASSWORD'),
            "url": "destination.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
        },
        'destination_3': {
            "db_name": "database-name",
            "schema": "wms",
            "password": os.getenv('DB_PASSWORD'),
            "url": "destination.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql"            
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
            "table_name": "batch_process_table",
        }
    ]
}