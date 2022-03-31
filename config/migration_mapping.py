import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "team_leader_users_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'database-migration-service-prod'
        },
        'tables': [
            {
                'table_name': 'team_leader_users',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False,
                'batch_size': 10000,
            },
        ]
    }
}

settings = {
    'fastapi_server': True,
    'timezone': 'Asia/Kolkata',
    'notify': True,
    'encryption_store': {
        'url': os.getenv('ENCR_MONGO_URL'),
        'db_name': os.getenv('DB_NAME'),
        'collection_name': os.getenv('COLLECTION_NAME')
    },
    'logging_store': {
        'url': os.getenv('LOG_MONGO_URL'),
        'db_name': os.getenv('LOG_DB_NAME'),
        'collection_name': os.getenv('LOG_COLLECTION_NAME')
    },
    'slack_notif': {
        'slack_token': 'xoxb-667683339585-3192552509475-C0xJXwmmUUwrIe4FYA0pxv2N',
        'channel': "C035WQHD291"
    },
    'save_logs': False
}
