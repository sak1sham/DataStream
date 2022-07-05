import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    'source': {
        'source_type': 'mongo',
        'url': os.getenv('DOCUMENT_DB_URL'),
        'db_name': 'source-db-name',
        'certificate_file': 'rds-combined-ca-bundle.pem'
    },
    "destination": {
        's3_1': {
            'destination_type': 's3', 
            's3_bucket_name': 's3-bucket-name-1' 
        },
        's3_2': {
            'destination_type': 's3', 
            's3_bucket_name': 's3-bucket-name-2' 
        },
        'pgsql': {
            "db_name": "destination_database_name",
            "password": os.getenv('DB_PASSWORD'),
            "url": "pgsql.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
            "schema": "public"
        },
    },
    'collections': [
        {
            'collection_name': 'my-first-collection',
            'fields': {
                'created_ts': 'datetime',
                'updated_ts': 'datetime'
            },
            'bookmark': 'updated_ts',
            'cron': 'self-managed',
            'mode': 'syncing',
            'improper_bookmarks': False
        },
        {
            'collection_name': 'my-second-collection',
            'fields': {},
            'cron': 'self-managed',
            'mode': 'syncing',
        },
        {
            'collection_name': 'my-third-collection',
            'fields': {
                'priority': 'int',
            },
            'bookmark': False,
            'cron': 'self-managed',
            'mode': 'syncing',
            'improper_bookmarks': False
        },
        {
            'collection_name': 'my-fourth-collection',
            'fields': {},
            'bookmark': False,
            'cron': 'self-managed',
            'mode': 'syncing',
            'improper_bookmarks': False
        },
        {
            'collection_name': 'my-fifth-collection',
            'fields': {
                'incoming': 'bool',
                'private': 'bool',
                'user_id': 'int',
                '__v': 'int',
                'created_at': 'datetime',
                'updated_at': 'datetime',
                'created_ts': 'datetime',
                'updated_ts': 'datetime'
            },
            'bookmark': 'updated_ts',
            'cron': 'self-managed',
            'mode': 'syncing',
            'improper_bookmarks': False,
            'buffer_updation_lag':{
                'hours': 2,
            },
            'grace_updation_lag': {
                'days': 1
            },
        },
        {
            'collection_name': 'my-sixth-collection',
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
                'responded_at': 'datetime',
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
            'cron': 'self-managed',
            'mode': 'syncing',
            'improper_bookmarks': False,
            'buffer_updation_lag': {
                'days': 1
            },
            'grace_updation_lag': {
                'days': 1
            }
        }
    ]
}