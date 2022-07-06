import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    'source': {
        'source_type': 'pgsql',
        'url': os.getenv('CRMDB_URL'),
        "db_name": "database-name",
        'username': os.getenv('DB_USERNAME'),
        'password': os.getenv('DB_PASSWORD')
    },
    "destination": {
        # 'destination_1': {
        #     "db_name": "database-name",
        #     "password": os.getenv('DB_PASSWORD'),
        #     "url": "destination.connection.url",
        #     "username": os.getenv('DB_USERNAME'),
        #     "destination_type": "pgsql",
        #     "schema": "crm"
        # },
        'destination_2': {
            "db_name": "database-name",
            "password": os.getenv('DB_PASSWORD'),
            "url": "destination.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
            "schema": "crm"
        },
        # 'destination_3': {
        #     "db_name": "database-name",
        #     "password": os.getenv('DB_PASSWORD'),
        #     "url": "destination.connection.url",
        #     "username": os.getenv('DB_USERNAME'),
        #     "destination_type": "pgsql",
        #     "schema": "crm"
        # }
    },
    'tables': [
        {
            'table_name': 'user_call_logs',
            'mode': 'mirroring',
            'primary_key': 'call_id',
            'primary_key_datatype': 'int',
            'cron': 'self-managed',
            'bookmark': 'updated_at_for_pipeline',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'strict': True,
            'buffer_updation_lag':{
                'hours': 2,
            },
            'grace_updation_lag': {
                'days': 1
            },
        }
    ]
}