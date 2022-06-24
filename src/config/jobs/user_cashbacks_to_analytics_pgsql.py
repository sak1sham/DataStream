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
        # 'ec2_1': {
        #     "db_name": "cmdb",
        #     "password": os.getenv('DB_PASSWORD'),
        #     "url": "dms2.citymall.dev",
        #     "username": os.getenv('DB_USERNAME'),
        #     "destination_type": "pgsql",
        #     "schema": "public"
        # },
        'ec2_2': {
            "db_name": "cmdb",
            "password": os.getenv('DB_PASSWORD'),
            "url": "dms1.citymall.dev",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
            "schema": "public"
        },
        # 'ec2_3': {
        #     "db_name": "cmdb",
        #     "password": os.getenv('DB_PASSWORD'),
        #     "url": "dms3.citymall.dev",
        #     "username": os.getenv('DB_USERNAME'),
        #     "destination_type": "pgsql",
        #     "schema": "public"
        # }
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
