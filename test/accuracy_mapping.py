import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "mongo_support_service_to_s3": {
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
                'collection_name': 'support_ticket_conversations',
                'fields': {
                    'incoming': 'bool',
                    'private': 'bool',
                    'freshdesk_user_id': 'int',
                    '__v': 'int',
                    'created_at': 'datetime',
                    'updated_at': 'datetime'
                },
                'bookmark': 'updated_at',
                'archive': False,
                'to_partition': True,
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
                },
                'bookmark': 'updated_at',
                'archive': False,
                'to_partition': True,
            },
            {
                'collection_name': 'support_tickets_rating',
                'fields': {
                    'rating': 'int',
                    '__v': 'int',
                    'updatedAt': 'datetime',
                    'createdAt': 'datetime',
                },
                'bookmark': 'updatedAt',
                'archive': False,
                'to_partition': True,
            },
        ]
    },
}

settings = {
    'encryption_store': {
        'url': 'mongodb+srv://manish:KlSh0bX605PY509h@cluster0.ebwdr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
        'db_name': 'migration_update_check',
        'collection_name': 'migration_update_check'
    }
}
