list_databases = [
    {
        'db_name': 'support-service',
        'url': 'mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservice.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
        'time_hour': 8,
        'time_minute': 14,
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
        'time_hour': 8,
        'time_minute': 13,
        'url': 'mongodb+srv://saksham:racSIVp6VpWde0Co@scrapping.k9dmh.mongodb.net/scrapping?retryWrites=true&w=majority',
        'fetch_type': 'all'    
    }
]