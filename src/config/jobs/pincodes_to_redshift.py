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
            'table_name': 'pincodes',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'lob_fields_length': {
                'district': 3036,
                'state_name': 3036,
                'city': 3036,
                'circle_name': 3036,
                'region_name': 3036,
                'division_name': 3036,
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