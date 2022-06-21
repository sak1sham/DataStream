import os
from dotenv import load_dotenv
load_dotenv()

mapping = { 
    'source': { 
        'source_type': 'pgsql', 
        'url': os.getenv('CMDB_URL'), 
        'db_name': 'wmsdb', 
        'username': os.getenv('DB_USERNAME'), 
        'password': os.getenv('DB_PASSWORD') 
    }, 
    "destination": {
        'ec2_1': {
            "db_name": "wmsdb",
            "schema": "public",
            "password": os.getenv('DB_PASSWORD'),
            "url": "dms2.citymall.dev",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
        },
        'ec2_2': {
            "db_name": "wmsdb",
            "schema": "public",
            "password": os.getenv('DB_PASSWORD'),
            "url": "dms1.citymall.dev",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
        }
    },
    'tables': [ 
        {
            'table_name': 'rm_skus_parent_batch_wise', 
            'cron': 'self-managed', 
            'mode': 'mirroring',
            'primary_key': 'id',
            'primary_key_datatype': 'int', 
            'bookmark': 'updated_at_for_pipeline', 
            'improper_bookmarks': False, 
            'batch_size': 10000, 
            'strict': True, 
            'buffer_updation_lag': {
                'hours': 2,
            } ,
            'grace_updation_lag': {
                'days': 1
            },
        },
    ]
}
