mapping = { 
    'source': { 
        'source_type': 'pgsql', 
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com', 
        'db_name': 'cmdb', 
        'username': 'saksham_garg', 
        'password': '3y5HMs^2qy%&Kma' 
    }, 
    'destination': { 
        's3': {
            'destination_type': 's3', 
            's3_bucket_name': 'database-migration-service-prod' 
        }
    },
    'tables': [ 
        {
            'table_name': 'user_cashback_transactions',
            'cron': '* * * * * 22 0 0',
            'mode': 'syncing',
            'primary_key': 'id',
            'primary_key_datatype': 'uuid',
            'partition_col': 'created_at',
            'partition_col_format': 'datetime',
            'bookmark': 'updated_at_for_pipeline',
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
