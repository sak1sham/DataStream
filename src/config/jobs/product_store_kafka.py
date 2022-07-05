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
        'consumer_group_id': 'product_store_dms',
        'kafka_server': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
        'db_name': 'audit_logs'
    },
    'destination': { 
        's3': {
            'destination_type': 's3', 
            's3_bucket_name': 'dms-kafka',
        }
    },
    'topics': [
        {
            'topic_name': 'pstore-iudevent-queue',
            'fields': {
                'sku_id': 'str',
                'catalogue_name': 'int',
                'warehouse_name': 'str',
                'product_status_from': 'str',
                'product_status_to': 'str',
                'timestamp': 'datetime',
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
        'url': os.getenv('SOURCE_DB_URL'),
        "db_name": "database-name",
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
    record['sku_id'] = None
    record['warehouse_name'] = None
    record['catalogue_name'] = None
    record['product_status_to'] = None
    record['timestamp'] = None
    record['product_status_from'] = None
    if('payload' in record.keys()):
        if(isinstance(record['payload'], dict)):
            if('sku_id' in record['payload'].keys()):
                record['sku_id'] = record['payload']['sku_id']
            if('warehouse_name' in record['payload'].keys()):
                record['warehouse_name'] = record['payload']['warehouse_name']
            if('catalogue_name' in record['payload'].keys()):
                record['catalogue_name'] = record['payload']['catalogue_name']
            if('product_status_to' in record['payload'].keys()):
                record['product_status_to'] = record['payload']['product_status_to']
            if('timestamp' in record['payload'].keys()):
                record['timestamp'] = record['payload']['timestamp']
            if('product_status_from' in record['payload'].keys()):
                record['product_status_from'] = record['payload']['product_status_from']
        record['payload'] = json.dumps(record['payload'])
    return record