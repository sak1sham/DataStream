import os
from dotenv import load_dotenv
load_dotenv()

mapping = [
    {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'cm',
            'password': 'cm'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'data-migration-server'
        },
        'tables':[
            {
                'table_name': 'localities_live',
                'cron': '* * * * * 22 0 0',
                'to_partition': True,
                'partition_col': 'migration_snapshot_date',
                'partition_col_format': 'datetime',
                'is_dump': True
            }
        ]
    },
    {
        'source': {
            'source_type': 'mongo',
            'url': 'mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservice.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
            'db_name': 'support-service'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'data-migration-server'
        },
        'collections': [
            {
                'collection_name': 'leader_kyc',
                'fields': {},
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 22 0 0',
                'to_partition': True
            },
            {
                'collection_name': 'support_form_items',
                'fields': {},
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 22 0 0',
                'to_partition': True,
                'is_dump': True,
                'partition_col': 'migration_snapshot_date'
            },
            {
                'collection_name': 'support_items',
                'fields': {
                    'priority': 'int',
                },
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 22 0 0',
                'to_partition': True
            },
            {
                'collection_name': 'support_list',
                'fields': {},
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 22 0 0',
                'to_partition': True
            },
            {
                'collection_name': 'support_ticket_conversations',
                'fields': {
                    'incoming': 'bool',
                    'private': 'bool',
                    'freshdesk_user_id': 'int',
                    '__v': 'int'
                },
                'bookmark': 'updated_at',
                'bookmark_format': '%Y-%m-%dT%H:%M:%S.%fZ',
                'archive': False,
                'cron': '* * * * * 22 30 0',
                'to_partition': True,
            },
            {
                'collection_name': 'support_tickets',
                'fields': {
                    'spam': 'bool',
                    'priority': 'int',
                    'source': 'int',
                    'status': 'int',
                    'is_escalated': 'bool',
                    'nr_escalated': 'bool',
                    'fr_escalated': 'bool',
                    '__v': 'int'
                },
                'bookmark': 'updated_at',
                'archive': False,
                'cron': '* * * * * 23 0 0',
                'to_partition': True,
            },
            {
                'collection_name': 'support_tickets_rating',
                'fields': {
                    'rating': 'int',
                    '__v': 'int'
                },
                'bookmark': 'updatedAt',
                'archive': False,
                'cron': '* * * * * 22 0 0',
                'to_partition': True,
            },
            {
                'collection_name': 'webhook_error_logs',
                'fields': {},
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 22 0 0',
                'to_partition': True
            },
        ]
    }
]

encryption_store = {
    'url': os.getenv('ENCR_MONGO_URL'),
    'db_name': os.getenv('DB_NAME'),
    'collection_name': os.getenv('COLLECTION_NAME')
}