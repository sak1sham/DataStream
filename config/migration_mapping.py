import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "entire_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'cm',
            'password': 'cm'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [
            {
                'table_name': '*',
                'cron': '2022 2 25 * * 20 17 0',
                'to_partition': True,
                'partition_col': 'migration_snapshot_date',
                'partition_col_format': 'datetime',
                'is_dump': True,
            }
        ]
    },
    'fastapi_server': True,
    'timezone': 'Asia/Kolkata',
}

encryption_store = {
    'url': os.getenv('ENCR_MONGO_URL'),
    'db_name': os.getenv('DB_NAME'),
    'collection_name': os.getenv('COLLECTION_NAME')
}
