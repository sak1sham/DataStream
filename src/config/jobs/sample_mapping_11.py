import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    'source': {
        'source_type': 'pgsql',
        'url': os.getenv('SOURCE_DB_URL'),
        "db_name": "database-name",
        'username': os.getenv('DB_USERNAME'),
        'password': os.getenv('DB_PASSWORD')
    },
    "destination": {
        'destination_1': {
            "destination_type": "pgsql",
            "db_name": "database-name",
            "password": os.getenv('DB_PASSWORD'),
            "url": "destination.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "schema": "public"
        },
        'destination_2': {
            "destination_type": "pgsql",
            "db_name": "database-name",
            "password": os.getenv('DB_PASSWORD'),
            "url": "destination.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "schema": "public"
        },
        'destination_3': {
            "destination_type": "pgsql",
            "db_name": "database-name",
            "password": os.getenv('DB_PASSWORD'),
            "url": "destination.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "schema": "public"
        }
    },
    'tables': [
        {
            'table_name': 'tbl_admin',
            'cron': 'self-managed',
            'mode': 'mirroring',
            'primary_key': 'admin_id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at_for_pipeline',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'strict': True,
            'grace_updation_lag': {
                'days': 1
            },
        },
    ]
}