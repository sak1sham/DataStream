## Format for cron expression:
## year, month, day, week, day_of_week, hour, minute, second

'''
    source_type = 'mongo' for MongoDB, 'mysql', 'api

    MongoDB:
        db_name = Name of the database to fetch from mongoDB
        url = connection url for the database 
        cron = cron expression for scheduling
        fetch_type = 'all' (to fetch all collections), 'selected' (to fetch some collections)
        collections = list of collections to fetch from db_name with their properties (Optional)
        collection_name = name of the collection (Optional)
        fields = data types of the fields (Optional)
'''

mapping = [
    {
        'source_type': 'mongo',
        'db_name': 'support-service',
        'url': 'mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservice.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
        'cron': '* * * * * 11-12 */2 0',
        'fetch_type': 'selected',
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