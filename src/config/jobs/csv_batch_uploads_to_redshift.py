mapping = {
    'source': {
        'source_type': 'pgsql',
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'cmdb',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
    },
    "destination": {
        'redshift': {
            'destination_type': 'redshift',
            'host': 'cm-redshift-1.cyl4ilkelm5m.ap-south-1.redshift.amazonaws.com',
            'database': 'cmwh',
            'user': 'cmadmin',
            'password': 'kgDzH6Zy5xZ6HHx',
            's3_bucket_name': 'database-migration-service-prod',
        }
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
                'days': 1
            },
        },
    ]
}