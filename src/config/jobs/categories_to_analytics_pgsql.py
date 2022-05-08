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
        'url': '3.108.43.163',
        'db_name': 'dms',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
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
                'hours': 2
            },
        }
    ]
}