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
            'table_name': 'support_tickets_rating',
            'mode': 'syncing',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'cron': '* * * * * 22 20 0',
            'to_partition': True,
            'partition_col': 'created_at',
            'partition_col_format': 'datetime',
            'bookmark': 'updated_at',
        }
    ]
}