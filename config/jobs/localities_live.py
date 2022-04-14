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
            'table_name': 'localities_live',
            'cron': 'self-managed',
            'mode': 'dumping',
            'to_partition': True,
            'partition_col': 'migration_snapshot_date',
            'partition_col_format': 'datetime',
            'batch_size': 10000,
        },
    ]
}