import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    'source': {
        'source_type': 'pgsql',
        'url': os.getenv('SOURCE_DB_URL'),
        "db_name": "database-name",
        'username': os.getenv('DB_USERNAME'),
        'password': os.getenv('DB_PASSWORD')
    },
    "destination": {
        'redshift': {
            'destination_type': 'redshift',
            'host': os.getenv('REDSHIFT_URL'),
            'database': 'redshift-db-name',
            'user': os.getenv('REDSHIFT_USER'),
            'password': os.getenv('REDSHIFT_PASSWORD'),
            's3_bucket_name': 's3-bucket-name',
        }
    },
    'tables': [            
        {
            'table_name': 'orders',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'order_id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 100000,
            'lob_fields_length': {
                'review_c1': 65535,
                'review_c2': 65535,
                'tags': 65535,
                'tracking_info': 65535,
                'extra_info': 65535,
                'order_error_details': 65535,
                'payment_status': 3036,
                'address_pincode': 3036,
                'rp_order_id': 3036,
                'c1_formatted_address': 10240,
                'order_rescheduled_reason': 3036,
                'address_name': 3036,
                'order_state': 3036,
                'payment_id': 3036,
            },
            'buffer_updation_lag':{
                'hours': 2,
            },
            'grace_updation_lag': {
                'days': 1
            },
        },
    ]
}