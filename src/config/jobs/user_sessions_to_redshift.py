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
        'redshift': {
            'destination_type': 'redshift',
            'host': os.getenv('REDSHIFT_URL'),
            'database': 'redshift-db-name',
            'user': os.getenv('REDSHIFT_USER'),
            'password': os.getenv('REDSHIFT_PASSWORD'),
            's3_bucket_name': 's3-bucket-name',
        }
    },
    'tables': [ 
        {
            'table_name': 'user_sessions',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'id',
            'primary_key_datatype': 'uuid',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'lob_fields_length': {
                'user_id': 3036,
                'device_info': 65535,
                'unique_device_id': 65535,
                'idfa': 65535
            },
            'buffer_updation_lag':{
                'hours': 2,
            },
            'grace_updation_lag': {
                'days': 1
            },
        },
    ]
}
