'''
    source_type = 'mongo' for MongoDB, 'mysql', 'api

    MongoDB:
        db_name = Name of the database to fetch from mongoDB
        url = connection url for the database 
        cron = cron expression for scheduling. Format for cron expression: (year, month, day, week, day_of_week, hour, minute, second) as per https://apscheduler.readthedocs.io/en/v2.1.0/cronschedule.html
        collections = list of collections to fetch from db_name with their properties (Optional)
        collection_name = name of the collection (Optional)
        fields = data types of the fields (Optional)
        bookmark = False or some string value for field_name, which represents updation check date (Compulsory)
        bookmark_format = If bookmark field contains dates as strings, provide a string format of the format. Ex- "%Y-%m-%dT%H:%M:%S.%fZ" for dates like "2021-12-06T10:33:22Z"
    
    destination_type = 's3' or 'redshift'
    S3
        s3_bucket_name = name of s3 bucket
    archive = query to archive records
'''

mapping = [
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
                'archive': '',
                'cron': '* * * * * 7-19 */1 0'
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
                'archive': '',
                'cron': '* * * * * 7-19 */1 0'
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
                'archive': '',
                'cron': '* * * * * 7-19 */1 0'
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
                'archive': '',
                'cron': '* * * * * 7-19 */1 0'
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
                'archive': '',
                'cron': '* * * * * 7-19 */1 0'
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
                'archive': '',
                'cron': '* * * * * 7-19 */1 0'
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
                'archive': '',
                'cron': '* * * * * 7-19 */1 0'
            },
            {
                'collection_name': 'webhook_error_logs',
                'archive': '',
                'cron': '* * * * * 7-19 */1 0'
            }
        ]
    }
]

encryption_store = {
    'url': 'mongodb+srv://manish:KlSh0bX605PY509h@cluster0.ebwdr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
    'db_name': 'migration_update_check',
    'collection_name': 'migration_update_check'
}
encryption_store = {
    'url': 'mongodb+srv://sak1sham:abcd@cluster0.azvu4.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
    'db_name': 'migration_update_check',
    'collection_name': 'migration_update_check'
}