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
            'table_name': 'engine_snapshots',
            'mode': 'logging',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'cron': 'self-managed',
            'partition_col': 'created_at',
            'partition_col_format': 'datetime',
            'batch_size': 30,
        }
    ]
}
