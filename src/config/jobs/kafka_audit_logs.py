from typing import Dict
import json

mapping = {
    'source': {
        'source_type': 'kafka',
        'kafka_username': 'kafka-client-user',
        'kafka_password': 'JjZXllrTsb6KgOVM',
        'consumer_group_id': 'dms_group_kafka',
        'kafka_server': 'b-2.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-3.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-1.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-4.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-5.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096,b-6.cm-live-cluster.3980tb.c4.kafka.ap-south-1.amazonaws.com:9096',
        'db_name': 'audit_logs'
    },
    'destination': {
        'destination_type': 's3',
        's3_bucket_name': 'database-migration-service-prod',
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
                'lat': 'float',
                'lng': 'float',
                'fingerprint': 'str',
                'old_value': 'str',
                'date': 'datetime',
            },
            'cron': 'self-managed',
            'to_partition': True,
            'partition_col': 'date',
            'partition_col_format': ['datetime'],
            'col_rename': {
                'date': 'created_at',
            }
        },
    ],
    'testing': {
        'test_type': 'sql',
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'cmdb',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma',
        'table_name': 'audit_logs',
        'field_to_compare': 'created_at',
        'field_format': 'datetime',
    },
    'redis': {
        'url': 'redis://cm-app-impressions-live-sa.t8se9i.0001.aps1.cache.amazonaws.com',
        'password': ''
    }
}


def get_table_name(record: Dict = {}) -> str:
    if record['type'] == 'writeAudit':
        return 'audit_logs'
    else:
        return 'product_audit_logs'


def process_dict(record: Dict = {}) -> Dict:
    record['user_type'] = None
    record['user_id_text'] = None
    record['user_id_bigint'] = None
    record['entity'] = None
    record['changes'] = None
    record['action'] = None
    if('payload' in record.keys()):
        if(isinstance(record['payload'], dict)):
            if('user_type' in record['payload'].keys()):
                record['user_type'] = record['payload']['user_type']
            if('user_id_text' in record['payload'].keys()):
                record['user_id_text'] = record['payload']['user_id_text']
            if('user_id_bigint' in record['payload'].keys()):
                record['user_id_bigint'] = record['payload']['user_id_bigint']
            if('entity' in record['payload'].keys()):
                record['entity'] = record['payload']['entity']
            if('changes' in record['payload'].keys()):
                record['changes'] = record['payload']['changes']
            if('action' in record['payload'].keys()):
                record['action'] = record['payload']['action']
        record['payload'] = json.dumps(record['payload'])
    return record