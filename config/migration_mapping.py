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
        
    
    destination_type = 's3' or 'redshift'
    S3
        s3_bucket_name = name of s3 bucket
    archive = query to archive records
'''

mapping = [
    {
        'source_type': 'mongo',
        'destination_type': 's3',
        's3_bucket_name': 'migration-service-temp',
        'db_name': 'support-service',
        'url': 'mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservice.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
        'cron': '* * * * * 14 42 0',
        'archive': '',
        'collections': [
            {
                'collection_name': 'support_ticket_conversations',
                'fields': {
                    'ticket_id': 'string',
                    'body': 'string',
                    'body_text': 'string',
                    'freshdesk_user_id': 'integer',
                    'from_email': 'string',
                    'created_at': 'string',
                    'updated_at': 'string',
                    '__v': 'integer'
                },
                'bookmark': 'updated_at',
                'bookmark_format': '2021-12-06T10:35:56Z'
            }
        ]
    }
]

encryption_store = {
    'url': 'mongodb+srv://sak1sham:abcd@cluster0.azvu4.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
    'db_name': 'migration_update_check',
    'collection_name': 'migration_update_check'
}