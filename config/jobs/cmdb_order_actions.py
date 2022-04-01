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
            'table_name': 'order_actions',
            'mode': 'logging',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'cron': '* * * * * 22 40 0',
            'to_partition': True,
            'partition_col': 'created_at',
            'partition_col_format': 'datetime',
            'batch_size': 10000,
        }
    ]
}