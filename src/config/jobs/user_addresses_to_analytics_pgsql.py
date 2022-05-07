mapping = {
    'source': {
        'source_type': 'sql',
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'cmdb',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
    },
    'destination': {
        'destination_type': 'pgsql',
        'url': '13.233.145.240',
        'db_name': 'dms',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
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
                'hours': 2
            },
        },
    ]
}
