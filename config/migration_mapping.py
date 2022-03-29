import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "mongo_s3_support": {
        'source': {
            'source_type': 'mongo',
            'url': 'mongodb://manish:ACVVCH7t7rqd8kB8@supportv2.cbo3ijdmzhje.ap-south-1.docdb.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&retryWrites=false',
            'db_name': 'support-service',
            'certificate_file': 'rds-combined-ca-bundle.pem'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'database-migration-service-prod',
        },
        'collections': [
            {
                'collection_name': 'support_form_items',
                'fields': {
                    'created_ts': 'datetime',
                    'updated_ts': 'datetime'
                },
                'bookmark': 'updated_ts',
                'cron': '* * * * * 1 0 0',
                'to_partition': True,
                'mode': 'syncing',
                'improper_bookmarks': False
            },
            {
                'collection_name': 'support_forms',
                'fields': {},
                'cron': '* * * * * 9 25 0',
                'to_partition': True,
                'mode': 'syncing',
            },
            {
                'collection_name': 'support_items',
                'fields': {
                    'priority': 'int',
                },
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 1 0 0',
                'to_partition': True,
                'mode': 'syncing',
                'improper_bookmarks': False
            },
            {
                'collection_name': 'support_list',
                'fields': {},
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 1 0 0',
                'to_partition': True,
                'mode': 'syncing',
                'improper_bookmarks': False
            },
            {
                'collection_name': 'support_ticket_conversations',
                'fields': {
                    'incoming': 'bool',
                    'private': 'bool',
                    'freshdesk_user_id': 'int',
                    '__v': 'int',
                    'created_at': 'datetime',
                    'updated_at': 'datetime',
                    'created_ts': 'datetime',
                    'updated_ts': 'datetime'
                },
                'bookmark': 'updated_ts',
                'archive': False,
                'cron': '* * * * * 1 5 0',
                'to_partition': True,
                'mode': 'syncing',
                'improper_bookmarks': False
            },
            {
                'collection_name': 'support_tickets',
                'fields': {
                    'created_at': 'datetime',
                    'spam': 'bool',
                    'priority': 'int',
                    'source': 'int',
                    'status': 'int',
                    'is_escalated': 'bool',
                    'updated_at': 'datetime',
                    'nr_escalated': 'bool',
                    'fr_escalated': 'bool',
                    '__v': 'int',
                    'agent_responded_at': 'datetime',
                    'cancelled_at':'datetime',
                    'closed_at':'datetime',
                    'due_by':'datetime',
                    'first_responded_at':'datetime',
                    'fr_due_by':'datetime',
                    'resolved_at':'datetime',
                    'status_updated_at':'datetime',
                    'created_ts': 'datetime',
                    'updated_ts': 'datetime'
                },
                'bookmark': 'updated_ts',
                'archive': False,
                'cron': '* * * * * 9 25 0',
                'to_partition': True,
                'mode': 'syncing',
                'improper_bookmarks': False
            },
            {
                'collection_name': 'support_tickets_rating',
                'fields': {
                    'rating': 'int',
                    '__v': 'int',
                    'updatedAt': 'datetime',
                    'createdAt': 'datetime',
                    'created_ts': 'datetime',
                    'updated_ts': 'datetime'
                },
                'bookmark': 'updated_ts',
                'archive': False,
                'cron': '* * * * * 1 0 0',
                'to_partition': True,
                'mode': 'syncing',
                'improper_bookmarks': False
            },
            {
                'collection_name': 'support_kafka_log',
                'fields': {},
                'archive': False,
                'cron': '* * * * * 1 0 0',
                'to_partition': True,
                'mode': 'syncing',
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