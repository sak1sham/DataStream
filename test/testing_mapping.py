import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "mongo_s3_support": {
        'source': {
            'source_type': 'mongo',
            'url': 'mongodb://manish:ACVVCH7t7rqd8kB8@supportv2.cbo3ijdmzhje.ap-south-1.docdb.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&retryWrites=false',
            'db_name': 'support-service'
        }, 
        'collections': [
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
                'improper_bookmarks': False
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


'''
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
                'improper_bookmarks': False
            },
            
'''