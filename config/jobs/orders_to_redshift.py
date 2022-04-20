mapping = {
    'source': {
        'source_type': 'sql',
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'cmdb',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
    },
    'destination': {
        'destination_type': 'redshift',
        'host': 'cm-redshift-1.cyl4ilkelm5m.ap-south-1.redshift.amazonaws.com',
        'database': 'cmwh',
        'user': 'cmadmin',
        'password': 'kgDzH6Zy5xZ6HHx',
        's3_bucket_name': 'database-migration-service-prod',
    },
    'tables': [            
        {
            'table_name': 'orders',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'order_id',
            'primary_key_datatype': 'int',
            'to_partition': True,
            'partition_col': 'created_at',
            'partition_col_format': 'datetime',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 100000,
            'lob_fields_length': {
                'cx_formatted_address': 3036,
                'touchpoint_formatted_address': 3036,
                'address_address1': 1024,
                'address_address2': 1024,
                'address_landmark': 1024,
                'order_cancellation_reason': 2048,
                'order_error': 2048,
                'order_cancellation_error': 2048,
                'tracking_info': 65535,
                'order_error_details': 65535,
                'extra_info': 65535,
                'review_cl': 65535,
                'review_cx': 65535,
                'tags': 65535,
            }
        },
    ]
}