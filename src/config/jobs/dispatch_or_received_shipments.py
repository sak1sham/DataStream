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
            'table_name': 'dispatch_or_received_shipments',
            'cron': 'self-managed',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'partition_col': 'scanned_at',
            'partition_col_format': 'datetime',
            'mode': 'logging',
            'batch_size': 100000,
            'buffer_updation_lag':{
                'hours': 2,
            },
            'grace_updation_lag': {
                'days': 1
            },
        }
    ]
}