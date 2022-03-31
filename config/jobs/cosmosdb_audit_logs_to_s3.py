mapping = {
    'source': {
        'source_type': 'mongo',
        'url': 'mongodb://cm-audit-logs:d1TCvFEVX4UbwuuYlM9EwJlkhV2K4NdWRyKASYn4cwj87157zUv73IGE85YAh2DsVJO7HrtWNzOvVvwWjn56ww==@cm-audit-logs.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@cm-audit-logs@',
        'db_name': 'test'
    },
    'destination': {
        'destination_type': 's3',
        's3_bucket_name': 'database-migration-service-prod',
    },
    'collections': [
        {
            'collection_name': 'audit_logs',
            'fields': {
                'user_id_bigint': 'int',
                'created_at': 'datetime',
                'lat': 'float',
                'long': 'float'
            },
            'mode': 'logging',
            'cron': '* * * * * 0 6 0',
            'to_partition': True,
            'partition_col': 'created_at',
            'partition_col_format': 'datetime',
            'bookmark': 'created_at',
            'batch_size': 150,
            'time_delay': 0.5,
        },
        {
            'collection_name': 'product_audit_logs',
            'fields': {
                'user_id_bigint': 'int',
                'created_at': 'datetime',
                'lat': 'float',
                'long': 'float'
            },
            'mode': 'logging',
            'cron': '* * * * * 0 6 0',
            'to_partition': True,
            'partition_col': 'created_at',
            'partition_col_format': 'datetime',
            'bookmark': 'created_at',
            'batch_size': 150,
            'time_delay': 0.5,
        }
    ]
},