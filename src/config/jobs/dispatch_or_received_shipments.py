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
            'table_name': 'dispatch_or_received_shipments',
            'cron': 'self-managed',
            'primary_key': 'shipment_id',
            'primary_key_datatype': 'int',
            'to_partition': True,
            'partition_col': 'scanned_at',
            'partition_col_format': 'datetime',
            'mode': 'logging',
            'batch_size': 10000,
        }
    ]
}