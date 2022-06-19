import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    'source': {
        'source_type': 'pgsql',
        'url': os.getenv('CMDB_URL'),
        'db_name': 'cmdb',
        'username': os.getenv('DB_USERNAME'),
        'password': os.getenv('DB_PASSWORD')
    },
    "destination": {
        'ec2_1': {
            "db_name": "cmdb",
            "schema": "public",
            "password": os.getenv('DB_PASSWORD'),
            "url": "15.206.171.84",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
        },
        'ec2_2': {
            "db_name": "cmdb",
            "schema": "public",
            "password": os.getenv('DB_PASSWORD'),
            "url": "13.233.225.181",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
        }
    },
    'tables': [
        {
            'table_name': 'user_cashbacks',
            'cron': 'self-managed',
            'mode': 'mirroring',
            'primary_key': 'id',
            'primary_key_datatype': 'uuid',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'partition_col': 'created_at',
            'partition_col_format': 'datetime',
            'batch_size': 10000,
            'strict': True,
            'buffer_updation_lag':{
                'hours': 2,
            },
            'grace_updation_lag': {
                'days': 1
            },
        },
    ]
}
