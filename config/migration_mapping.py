import os
import datetime
import pytz
IST_tz = pytz.timezone('Asia/Kolkata')

from dotenv import load_dotenv
load_dotenv()

'''
    migration mapping is a list of DS for data-pipeline start points. Structure of each DS:
    'source': {
        'source_type': 'mongo' or 'mysql' or 'api',
        'url': ''
        'db_name': ''
    },
    'destination': {
        'destination_type': 's3' or 'redshift',
        's3_bucket_name': ''
    },
    (IF SOURCE IS SQL)
    'tables': [
        {
            'table_name': '',
            'bookmark': False or 'field_name' (optional, for example - 'updated_at'),
            'bookmark_format': '' (optional, for example- "%Y-%m-%dT%H:%M:%S.%fZ" for dates like "2021-12-06T10:33:22Z"),
            'uniqueness': string or list of unique specifiers for records (Optional, needed if bookmark is False)
            'archive': "Mongodb_query" or False,
            'cron': '* * * * * 7-19 */1 0' (as per guidelines at https://apscheduler.readthedocs.io/en/v2.1.0/cronschedule.html - (year, month, day, week, day_of_week, hour, minute, second))
            'to_partition': True or False (Default),
            'partition_col': False or '' name of the datetime column
            'partition_col_format': '' (Optional, for example- "%Y-%m-%dT%H:%M:%S.%fZ" for dates like "2021-12-06T10:33:22Z")
        }
    ],
    (IF SOURCE IS MONGODB)
    'collections': [
        {
            'collection_name': '',
            'fields': {
                'field_1': 'integer' or 'string'
                ...
            },
            'bookmark': False or 'field_name' (optional, for example - 'updated_at'),
            'bookmark_format': '' (optional, for example- "%Y-%m-%dT%H:%M:%S.%fZ" for dates like "2021-12-06T10:33:22Z"),
            'archive': "Mongodb_query" or False,
            'cron': '* * * * * 7-19 */1 0' (as per guidelines at https://apscheduler.readthedocs.io/en/v2.1.0/cronschedule.html - (year, month, day, week, day_of_week, hour, minute, second))
            'to_partition': True or False (Default),
            'partition_col': False or '' name of the datetime column
            'partition_col_format': '' (Optional, for example- "%Y-%m-%dT%H:%M:%S.%fZ" for dates like "2021-12-06T10:33:22Z")
        }
    ]
'''

mapping = [
    {
        'source': {
            'source_type': 'sql',
            'url': 'postgresql://localhost/postgres',
            'db_name': 'postgres',
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp-2',
        },
        'tables': [
            {
                'table_name': 'phonebook',
                'uniqueness': 'firstname',
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 7-21 */1 10',
                'to_partition': False,
                'partition_col': False
            }
        ]
    },
    {   
        'source': {
            'source_type': 'mongo',
            'url': 'mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservice.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
            'db_name': 'support-service',
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp-2',
        },
        'collections': [
            {
                'collection_name': 'leader_kyc',
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 7-19 */1 0',
                'to_partition': False,
                'partition_col': False
            },
            {
                'collection_name': 'support_form_items',
                'fields': {
                    'form_item_id': 'string',
                    'label_hi': 'string',
                    'key': 'string',
                    'input_type': 'string',
                    'input_params': 'string',
                },
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 7-19 */1 0',
                'to_partition': False,
                'partition_col': False
            },
            {
                'collection_name': 'support_items',
                'fields': {
                    'next_action': 'string',
                    'department': 'string',
                    'item_text_en': 'string',
                    'priority': 'string',
                    'item_text': 'string',
                    'item_id': 'string',
                    'item_type': 'string',
                    'next_item_id': 'string'
                },
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 7-19 */1 0',
                'partition_col': False
            },
            {
                'collection_name': 'support_list',
                'fields': {
                    'list_id': 'string',
                    'list_heading_hi': 'string',
                    'list_subheading': 'string',
                    'list_subheading_priority': 'string'
                },
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 7-19 */1 0',
                'partition_col': False
            },
            {
                'collection_name': 'support_ticket_conversations',
                'fields': {
                    'ticket_id': 'string',
                    'body': 'string',
                    'body_text': 'string',
                    'freshdesk_user_id': 'string',
                    'from_email': 'string',
                    'created_at': 'string',
                    'updated_at': 'string',
                    '__v': 'string'
                },
                'bookmark': 'updated_at',
                'bookmark_format': '%Y-%m-%dT%H:%M:%S.%fZ',
                'archive': False,
                'cron': '* * * * * 7-19 */1 0',
                'partition_col': False
            },
            {
                'collection_name': 'support_tickets',
                'fields': {
                    'user_type': 'string',
                    'user_id': 'string',
                    'ticket_id': 'string',
                    'priority': 'string',
                    'requester_id': 'string',
                    'source': 'string',
                    'status': 'string',
                    'subject': 'string',
                    'description': 'string',
                    'type': 'string',
                    'due_by': 'string',
                    'fr_due_by': 'string',
                    '__v': 'string',
                    'csat_status': 'string'
                },
                'bookmark': 'updated_at',
                'archive': False,
                'cron': '* * * * * 7-19 */1 0',
                'partition_col': False
            },
            {
                'collection_name': 'support_tickets_rating',
                'fields': {
                    'ticket_id': 'string',
                    'reopen_at': 'string',
                    'rating': 'string',
                    '__v': 'string',
                },
                'bookmark': 'updated_at',
                'archive': False,
                'cron': '* * * * * 7-19 */1 0',
                'partition_col': False
            },
            {
                'collection_name': 'webhook_error_logs',
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 7-19 */1 0',
                'partition_col': False
            }
        ]
    }
]

encryption_store = {
    'url': os.getenv('ENCR_MONGO_URL'),
    'db_name': os.getenv('DB_NAME'),
    'collection_name': os.getenv('COLLECTION_NAME')
}