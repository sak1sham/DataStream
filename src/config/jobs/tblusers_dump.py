mapping = {
    'source': {
        'source_type': 'pgsql',
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'cmdb',
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
            'table_name': 'tbl_user',
            'mode': 'dumping',
            'partition_col': 'migration_snapshot_date',
            'partition_col_format': 'datetime',
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