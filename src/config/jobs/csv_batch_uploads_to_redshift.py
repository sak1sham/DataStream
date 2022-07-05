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
            'database': 'cmwh',
            'user': os.getenv('REDSHIFT_USER'),
            'password': os.getenv('REDSHIFT_PASSWORD'),
            's3_bucket_name': 'database-migration-service-prod',
        }
    },
    'tables': [            
        {
            'table_name': 'csv_batch_uploads',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'batch_id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'lob_fields_length': {
                'batch_status': 65535,
                'validation_summary': 65535,
                'extra_info': 65535,
                'functions_key': 3036,
                'validate_function': 3036,
                'run_function': 3036,
                'errors': 3036,
                'output_key': 3036,
                'upload_key': 3036,
                'rollback_function': 3036,
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