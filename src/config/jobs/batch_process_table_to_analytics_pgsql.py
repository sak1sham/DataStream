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
            'table_name': 'batch_process_table',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'lob_fields_length': {
                'status_result': 65535,
                'current_row': 65535,
                'run_result': 65535,
                'errors': 10240,
                'validation_error': 10240,
            },
            'grace_updation_lag': {
                'days': 1
            },
            'buffer_updation_lag':{
                'hours': 2,
            }
        },
    ]
}