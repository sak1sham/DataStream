from typing import Dict
import json

import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    'source': {
        'source_type': 'kafka',
        'kafka_username': os.getenv('KAFKA_USERNAME'),
        'kafka_password': os.getenv('KAFKA_PASSWORD'),
        'consumer_group_id': 'dms_kafka_consumer_group',
        'kafka_server': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
        'db_name': 'audit_logs'
    },
    'destination': { 
        's3': {
            'destination_type': 's3', 
            's3_bucket_name': 's3-bucket-name',
            's3_suffix': '_2'
        }
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
            'partition_col': 'date',
            'partition_col_format': ['datetime'],
            'col_rename': {
                'date': 'created_at',
            }
        },
    ],
    'redis': {
        'url': 'redis-url',
        'password': 'redis-password'
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
    if('payload' in record.keys()):
        if(isinstance(record['payload'], dict)):
            record['user_type'] = record['payload']['user_type']
            record['user_id_text'] = record['payload']['user_id_text']
        record['payload'] = json.dumps(record['payload'])
    return record