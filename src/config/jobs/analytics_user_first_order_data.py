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
            'table_name': 'analytics.user_first_order_data',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'user_id',
            'primary_key_datatype': 'int',
            'to_partition': True,
            'partition_col': 'first_order_created_date',
            'partition_col_format': 'datetime',
            'bookmark': 'updated_at_for_pipeline',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'buffer_updation_lag': {
                'hours': 2,
            } ,
            'grace_updation_lag': {
                'days': 1
            },
        },
    ]
}
