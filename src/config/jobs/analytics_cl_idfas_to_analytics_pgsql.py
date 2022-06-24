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
        #     "schema": "analytics"
        # },
        'ec2_2': {
            "db_name": "cmdb",
            "password": os.getenv('DB_PASSWORD'),
            "url": "dms1.citymall.dev",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
            "schema": "analytics"
        },
        # 'ec2_3': {
        #     "db_name": "cmdb",
        #     "password": os.getenv('DB_PASSWORD'),
        #     "url": "dms3.citymall.dev",
        #     "username": os.getenv('DB_USERNAME'),
        #     "destination_type": "pgsql",
        #     "schema": "analytics"
        # }
    },
    'tables': [            
        {
            'table_name': 'analytics.cl_idfas',
            'cron': 'self-managed',
            'mode': 'mirroring',
            'primary_key': 'cl_id',
            'primary_key_datatype': 'str',
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
        },
    ]
}