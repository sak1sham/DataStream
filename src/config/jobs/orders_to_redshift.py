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
            'batch_size': 10000,
            'lob_fields_length': {
                'review_cl': 65535,
                'review_cx': 65535,
                'tags': 65535,
                'tracking_info': 65535,
                'extra_info': 65535,
                'order_error_details': 65535,
                'payment_status': 3036,
                'address_pincode': 3036,
                'razorpay_order_id': 3036,
                'cx_formatted_address': 10240,
                'order_rescheduled_reason': 3036,
                'address_name': 3036,
                'touchpoint_micromarket': 3036,
                'created_by': 3036,
                'created_by_name': 10240,
                'created_by_type': 3036,
                'invoice_number': 3036,
                'order_cancellation_reason': 3036,
                'address_state': 3036,
                'address_address2': 10240,
                'address_address1': 3036,
                'address_landmark': 3036,
                'paytm_txn_token': 3036,
                'paytm_txn_status': 3036,
                'address_phone_number': 3036,
                'order_warehouse_name': 3036,
                'online_payment_status': 3036,
                'idfa': 3036,
                'price_drop_coupon_code': 3036,
                'order_catalogue_name': 3036,
                'address_city': 3036,
                'cart_id': 3036,
                'spoke_name': 3036,
                'juspay_order_id': 3036,
                'juspay_payment_id': 3036,
                'juspay_txn_id': 3036,
                'locality': 3036,
                'order_cancellation_error': 3036,
                'paytm_order_id': 3036,
                'order_status': 3036,
                'order_state': 3036,
                'touchpoint_formatted_address': 10240,
                'razorpay_payment_id': 3036,
                'user_language': 3036,
                'team_leader': 3036,
                'order_error': 3036,
                'coupon_code': 3036,
                'razorpay_signature': 3036,
            },
            'buffer_updation_lag':{
                'hours': 2,
            }
        },
    ]
}