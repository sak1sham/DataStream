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
    'destination': { 
        's3': {
            'destination_type': 's3', 
            's3_bucket_name': 'database-migration-service-prod' 
        }
    },
    'tables': [
        {
            'table_name': 'team_leaders',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'user_id',
            'primary_key_datatype': 'int',
            'partition_col': 'created_at',
            'partition_col_format': 'datetime',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'buffer_updation_lag':{
                'hours': 2,
            },
            'grace_updation_lag': {
                'days': 1
            },
        },
    ]
}