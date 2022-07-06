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
            'table_name': 'categories',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'category_id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'lob_fields_length': {
                'original_images': 65535,
                'category_tags': 65535,
                'category_url': 1024,
                'child_link_cl': 3036,
                'category_name_hi': 3036,
                'category_name_en': 3036,
                'category_image': 3036,
                'category_type': 3036,
                'child_type': 3036,
                'child_link_cx': 3036,
                'category_image_active': 3036,
                'category_image_non_active': 3036,
            },
            'buffer_updation_lag':{
                'hours': 2,
            },
            'grace_updation_lag': {
                'days': 1
            },
        }
    ]
}