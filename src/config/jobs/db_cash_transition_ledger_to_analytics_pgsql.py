mapping = {
    'source': {
        'source_type': 'sql',
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'cmdb',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
    },
    'destination': {
        'destination_type': 'pgsql',
        'url': '3.108.43.163',
        'db_name': 'dms',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
    },
    'tables': [            
        {
            'table_name': 'db_cash_transition_ledger',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'lob_fields_length': {
                'transaction_details': 65535,
                'type': 3036,
                'spoke_name': 3036,
                'route_id': 3036,
                'transition': 3036,
                'db_phone': 3036,
            },
            'buffer_updation_lag':{
                'hours': 2,
            },
            'grace_updation_lag': {
                'hours': 4
            },
        },
    ]
}