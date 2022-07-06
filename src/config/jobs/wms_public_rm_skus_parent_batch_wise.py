import os
from dotenv import load_dotenv
load_dotenv()

mapping = { 
    'source': { 
        'source_type': 'pgsql', 
        'url': os.getenv('SOURCE_DB_URL'), 
        'db_name': 'db-name', 
        'username': os.getenv('DB_USERNAME'), 
        'password': os.getenv('DB_PASSWORD') 
    }, 
    'destination': { 
        's3': {
            'destination_type': 's3', 
            's3_bucket_name': 's3-bucket-name' 
        }
    },
    'tables': [ 
        {
            'table_name': 'rm_skus_parent_batch_wise', 
            'cron': 'self-managed', 
            'mode': 'syncing',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'partition_col': 'grn_date',
            'partition_col_format': 'datetime',
            'bookmark': 'updated_at_for_pipeline', 
            'improper_bookmarks': False, 
            'batch_size': 10000, 
            'buffer_updation_lag': {
                'hours': 2,
            } ,
            'grace_updation_lag': {
                'days': 1
            },
        },
    ]
}
