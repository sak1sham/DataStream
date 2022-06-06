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
        'specifications': [
            {
                'url': '3.108.43.163',
                'db_name': 'dms',
                'username': 'saksham_garg',
                'password': '3y5HMs^2qy%&Kma'
            }
        ]
    },
    'tables': [
        {
            'table_name': 'localities_live',
            'cron': 'self-managed',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'mode': 'syncing',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'buffer_updation_lag':{
                'hours': 2,
            },
            'grace_updation_lag': {
                'days': 1
            },
        },
    ]
}