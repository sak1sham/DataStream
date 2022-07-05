import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "destination": {
        'ec2_1': {
            "db_name": "database-name",
            "schema": "wms",
            "password": os.getenv('DB_PASSWORD'),
            "url": "destination.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
        },
        'ec2_2': {
            "db_name": "database-name",
            "schema": "wms",
            "password": os.getenv('DB_PASSWORD'),
            "url": "destination.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
        },
        'ec2_3': {
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
            "batch_size": 100000,
            'strict': True,
            "cron": "self-managed",
            "mode": "logging",
            "primary_key": "id",
            "primary_key_datatype": "int",
            "table_name": "rm_events",
        }
    ]
}