import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "entire_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'cm',
            'password': 'cm'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [
            {
                'table_name': '*',
                'exclude_tables': ['public.inventory_snapshot_record', 'public.inventory_snapshot_wms', 'public.bd_leader_mapping_change_logs', 'public.events_staging_queue', 'public.stream_follows', 'public.user_segment_tags', 'public.notifications', 'public.order_actions', 'public.order_items', 'public.orders', 'public.team_leaders', 'public.products', 'public.product_master'],
                'cron': '2022 2 25 * * 20 24 0',
                'to_partition': True,
                'partition_col': 'migration_snapshot_date',
                'partition_col_format': 'datetime',
                'is_dump': True,
            }
        ]
    },
    'fastapi_server': True,
    'timezone': 'Asia/Kolkata',
}

encryption_store = {
    'url': os.getenv('ENCR_MONGO_URL'),
    'db_name': os.getenv('DB_NAME'),
    'collection_name': os.getenv('COLLECTION_NAME')
}
