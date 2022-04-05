mapping = {
    'kafka_audits': {
        'source': {
            'source_type': 'kafka',
            'kafka_username': 'kafka-client-user',
            'kafka_password': 'JjZXllrTsb6KgOVM',
            'consumer_group_id': 'audit_logs_consumer',
            'kafka_server': 'b-2.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-3.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-1.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-4.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-5.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-6.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096',
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'learning-migrationservice',
        },
        'topics': [
            {
                'topic_name': 'audit_logs',
                'fields': {
                    'user_type': 'str',
                    'user_id_bigint': 'int',
                    'user_id_text': 'str',
                    'action': 'str',
                    'entity': 'str',
                    'changes': 'str',
                    'created_at': 'datetime',
                    'lat': 'int',
                    'lng': 'int',
                    'fingerprint': 'str',
                    'old_value': 'str',
                },
                'cron': '* * * * * 22 0 0',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': ['datetime']
            },
        ]
    }
}