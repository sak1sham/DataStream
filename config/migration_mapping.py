'''
    source_type = 'mongo' for MongoDB, 'mysql', 'api

    MongoDB:
        db_name = Name of the database to fetch from mongoDB
        url = connection url for the database 
        cron = cron expression for scheduling. Format for cron expression: (year, month, day, week, day_of_week, hour, minute, second) as per https://apscheduler.readthedocs.io/en/v2.1.0/cronschedule.html
        collections = list of collections to fetch from db_name with their properties (Optional)
        collection_name = name of the collection (Optional)
        fields = data types of the fields (Optional)
    
    destination_type = 's3' or 'redshift' or 'local'
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
        'cron': '* * * * * 9-19 */1 0',
        'archive': '',
        'collections': [
            {
                'collection_name': 'support_form_items',
                'fields': {
                    'form_item_id': 'string',
                    'label_hi': 'string',
                    'key': 'string',
                    'input_type': 'string',
                    'input_params': 'string'
                }
            },
            {
                'collection_name': 'support_items',
                'fields': {
                    'next_action': 'string',
                    'department': 'string',
                    'item_text_en': 'string',
                    'priority': 'integer',
                    'item_text': 'string'
                }
            }
        ]
    }
]