mapping = {
    'source': {
        'source_type': 'sql',
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'cmdb',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
    },
    "destination": {
        'destination_type': 'redshift',
        'host': 'cm-redshift-1.cyl4ilkelm5m.ap-south-1.redshift.amazonaws.com',
        'database': 'cmwh',
        'user': 'cmadmin',
        'password': 'kgDzH6Zy5xZ6HHx',
        's3_bucket_name': 'data-migration-service-dev',
    },
    'tables': [            
        {
            'table_name': 'orders',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'order_id',
            'primary_key_datatype': 'int',
            'to_partition': False,
            'partition_col': 'created_at',
            'partition_col_format': 'datetime',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'lob_fields_length': {
                'address_name': 5000,
                'address_address1': 5000,
                'address_address2': 5000,
                'address_landmark': 5000,
                'tracking_info': 30000,
                'extra_info': 30000,
                'order_cancellation_error': 5000,
                'order_error': 5000,
                'order_error_details': 5000,
                'order_cancellation_reason': 5000,
                'tags': 45000,
                'touchpoint_formatted_address': 5000,
                'cx_formatted_address': 5000,
                'order_rescheduled_reason': 5000,
                'review_cx': 5000,
                'order_catalogue_name': 5000,
                'order_warehouse_name': 5000,
                'locality': 5000,
            }
        },
    ]
}