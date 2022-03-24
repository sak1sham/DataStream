import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "mongo_s3_support": {
        'source': {
            'source_type': 'mongo',
            'url': 'mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservicev2.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
            'db_name': 'support-service'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'database-migration-service-prod',
        },
        'collections': [
            {
                'collection_name': 'leader_kyc',
                'fields': {},
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 17 36 0',
                'to_partition': True,
                'mode': 'syncing',
                'improper_bookmarks': False
            },
            {
                'collection_name': 'support_form_items',
                'fields': {
                    'created_ts': 'datetime',
                    'updated_ts': 'datetime',
                },
                'bookmark': 'updated_ts',
                'archive': False,
                'cron': '* * * * * 17 36 0',
                'to_partition': True,
                'mode': 'syncing',
                'improper_bookmarks': False
            }
        ]
    },
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
