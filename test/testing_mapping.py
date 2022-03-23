import os
from dotenv import load_dotenv
load_dotenv()

'''
mapping = {
    "mongo_s3_support": {
        'source': {
            'source_type': 'mongo',
            'url': 'mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservicev2.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
            'db_name': 'support-service'
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
                    'agent_responded_at': 'datetime',
                    'cancelled_at':'datetime',
                    'closed_at':'datetime',
                    'due_by':'datetime',
                    'first_responded_at':'datetime',
                    'fr_due_by':'datetime',
                    'resolved_at':'datetime',
                    'status_updated_at':'datetime',
                },
                'bookmark': 'updated_at',
                'archive': False,
                'to_partition': True,
            }
        ]
    },
}
'''
'''
mapping = {
    "dispatch_or_received_shipments_cmdb_to_s3": {
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
                'table_name': 'dispatch_or_received_shipments',
                'cron': 'self-managed',
                'to_partition': True,
                'partition_col': 'scanned_at',
                'partition_col_format': 'datetime',
                'bookmark': 'scanned_at',
                'bookmark_creation': 'scanned_at',
                'mode': 'logging',
            }
        ]
    },
}
'''

mapping = {
    "mongo_s3_support": {
        'source': {
            'source_type': 'mongo',
            'url': 'mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservicev2.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
            'db_name': 'support-service'
        }, 
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'data-migration-server',
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
                    'updated_at': 'datetime',
                    'created_ts': 'datetime',
                    'updated_ts': 'datetime'
                },
                'bookmark': 'updated_ts',
                'archive': False,
                'cron': '* * * * * 10 42 0',
                'to_partition': True,
                'mode': 'syncing',
                'improper_bookmarks': True
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
                'to_partition': True,
                'mode': 'syncing',
                'improper_bookmarks': True
            }
        ]
    },
}

settings = {
    'encryption_store': {
        'url': 'mongodb+srv://manish:KlSh0bX605PY509h@cluster0.ebwdr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
        'db_name': 'test',
        'collection_name': 'test'
    }
}
