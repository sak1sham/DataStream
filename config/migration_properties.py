list_databases = [
    {
        'db_name': 'support-service',
        'fetch_type': 'selected',
        'collections': [
            {
                'collection_name': 'support_form_items',
                'format': {
                    'form_item_id': 'string',
                    'label_hi': 'string',
                    'key': 'string',
                    'input_type': 'string',
                    'input_params': 'string'
                }
            },
            {
                'collection_name': 'support_items',
                'format': {
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