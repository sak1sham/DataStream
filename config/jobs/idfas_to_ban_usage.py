mapping = {
    "from_cmdb_to_s3": {
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
                'table_name': 'idfas_to_ban_usage',
                'cron': '* * * * * 22 10 0',
                'mode': 'logging',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
            },
        ]
    }
}