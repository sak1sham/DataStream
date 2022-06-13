mapping = { 
    'source': { 
        'source_type': 'pgsql', 
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com', 
        'db_name': 'wmsdb', 
        'username': 'saksham_garg', 
        'password': '3y5HMs^2qy%&Kma' 
    }, 
    'destination': { 
        'destination_type': 's3', 
        'specifications': [
            {
                's3_bucket_name': 'database-migration-service-prod' 
            }
        ]
    }, 
    'tables': [ 
        {
            'table_name': 'rm_inventory_transactions', 
            'cron': 'self-managed', 
            'mode': 'logging',
            'primary_key': 'inventory_transaction_id',
            'primary_key_datatype': 'uuid',
            'partition_col': 'created_at',
            'partition_col_format': 'datetime',
            'batch_size': 100000, 
            'buffer_updation_lag': {
                'hours': 2,
            } ,
            'grace_updation_lag': {
                'days': 1
            },
        },
    ]
}
