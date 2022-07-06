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
            'table_name': 'user_addresses',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'lob_fields_length': {
                'geocode_geog': 65535,
                'gps_at_time_of_address_submit_geog': 65535,
                'geog': 65535,
                'manual_override_location_geog': 65535,
                'delivery_boy_location_geog': 65535,
                'whatsapp_location_geog': 65535,
                'device_location_geog': 65535,
                'pincode': 1024,
                'db_username': 4048,
                'formatted_address': 10240,
                'landmark': 4048,
                'name': 10240,
                'micromarket': 4048,
                'address2': 10240,
                'house_number': 4048,
                'address1': 10240,
                'geog_reverse_geocode_pincode': 4048,
                'geohash5': 4048,
                'phone_number': 4048,
                'geohash6': 4048,
                'state': 4048,
                'geohash': 4048,
                'city': 4048,
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

