import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "destination": {
        'destination_1': {
            "db_name": "database-name",
            "password": os.getenv('DB_PASSWORD'),
            "url": "destination.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
            "schema": "public"
        },
        'destination_2': {
            "db_name": "database-name",
            "password": os.getenv('DB_PASSWORD'),
            "url": "destination.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
            "schema": "public"
        },
        'destination_3': {
            "db_name": "database-name",
            "password": os.getenv('DB_PASSWORD'),
            "url": "destination.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
            "schema": "public"
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
            "table_name": "product_inventory_wms",
            "cron": "self-managed",
            "mode": "dumping",
            "batch_size": 10000,
            'strict': True,
        }
    ]
}