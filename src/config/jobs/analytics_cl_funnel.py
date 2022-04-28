mapping = {
    'source': {
        'source_type': 'sql',
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'cmdb',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
    },
    'destination': {
        'destination_type': 's3',
        's3_bucket_name': 'database-migration-service-prod'
    },
    'tables': [            
        {
            'table_name': 'analytics.cl_funnel',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'cl_id',
            'primary_key_datatype': 'str',
            'to_partition': True,
            'partition_col': 'cl_signup_date',
            'partition_col_format': 'datetime',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
        },
    ]
}