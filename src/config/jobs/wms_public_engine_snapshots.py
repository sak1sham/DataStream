mapping = { 
    'source': { 
        'source_type': 'pgsql', 
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com', 
        'db_name': 'wmsdb', 
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
            'table_name': 'engine_snapshots',
            'mode': 'logging',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'cron': 'self-managed',
            'partition_col': 'created_at',
            'partition_col_format': 'datetime',
            'batch_size': 30,
        }
    ]
}
