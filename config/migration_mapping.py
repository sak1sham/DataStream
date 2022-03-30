import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "public_admin_roles_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'data-migration-service-dev'
        },
        'tables': [
            {
                'table_name': 'admin_roles',
                'cron': 'self-managed',
                'mode': 'dumping',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                'batch_size': 10000,
            },
        ]
    }
}

settings = {
    'fastapi_server': False,
    'timezone': 'Asia/Kolkata',
    'notify': True,
    'encryption_store': {
        'url': os.getenv('ENCR_MONGO_URL'),
        'db_name': 'test',
        'collection_name': 'test'
    },
    'logging_store': {
        'url': os.getenv('LOG_MONGO_URL'),
        'db_name': 'test',
        'collection_name': 'test'
    },
    'slack_notif': {
        'slack_token': 'xoxb-667683339585-3192552509475-C0xJXwmmUUwrIe4FYA0pxv2N',
        'channel': "C035WQHD291"
    },
    'save_logs': False
}