## Format for cron expression:
## year, month, day, week, day_of_week, hour, minute, second
mapping = [
    {
        'db_name': 'support-service',
        'url': 'mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservice.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
        'cron': '* * * * * 16-17 */2 0',
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
    },
    {
        'db_name': 'spar',
        'cron': '* * * * * 16-17 */1 0',
        'url': 'mongodb+srv://saksham:racSIVp6VpWde0Co@scrapping.k9dmh.mongodb.net/scrapping?retryWrites=true&w=majority',
        'fetch_type': 'all'    
    }
]