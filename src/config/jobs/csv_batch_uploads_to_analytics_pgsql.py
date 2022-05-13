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
            'table_name': 'csv_batch_uploads',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'batch_id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'lob_fields_length': {
                'batch_status': 65535,
                'validation_summary': 65535,
                'extra_info': 65535,
                'functions_key': 3036,
                'validate_function': 3036,
                'run_function': 3036,
                'errors': 3036,
                'output_key': 3036,
                'upload_key': 3036,
                'rollback_function': 3036,
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