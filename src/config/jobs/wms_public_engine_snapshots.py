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
    'destination': { 
        's3': {
            'destination_type': 's3', 
            's3_bucket_name': 'database-migration-service-prod' 
        }
    },
    'tables': [ 
        {
            'table_name': 'engine_snapshots',
            'mode': 'logging',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'cron': 'self-managed',
            'partition_col': 'created_at',
            'partition_col_format': 'datetime',
            'batch_size': 10000,
        }
    ]
}
