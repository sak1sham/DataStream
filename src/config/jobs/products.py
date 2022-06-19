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
            'table_name': 'products',
            'cron': 'self-managed',
            'mode': 'dumping',
            'partition_col': 'migration_snapshot_date',
            'partition_col_format': 'datetime',
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
