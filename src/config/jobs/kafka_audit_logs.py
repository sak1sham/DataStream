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
        'consumer_group_id': 'dms_kafka_consumer_group_kafka',
        'kafka_server': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
        'db_name': 'audit_logs'
    },
    'destination': { 
        's3': {
            'destination_type': 's3', 
            's3_bucket_name': 'dms-kafka',
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
    'testing': {
        'test_type': 'pgsql',
        'url': os.getenv('CMDB_URL'),
        'db_name': 'cmdb',
        'username': os.getenv('DB_USERNAME'),
        'password': os.getenv('DB_PASSWORD'),
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