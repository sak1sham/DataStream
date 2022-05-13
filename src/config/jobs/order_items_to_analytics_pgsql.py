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
            'table_name': 'order_items',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'lob_fields_length': {
                'review_cl': 65535,
                'review_cx': 65535,
                'inventory_tx_ids': 65535,
                'metadata': 65535,

                'short_name': 3036,
                'name': 3036,
                
                'sku_id': 3036,
                'cancellation_reason': 3036,
                'catalogue_name': 3036,
                'warehouse_name': 3036,
                'cancelled_by': 3036,
                'cancellation_other_reason': 3036,
                'rescheduled_reason': 3036,
                'reverse_item_status': 3036,
                'undelivered_to_cx_reason': 3036,
                'order_item_status': 3036,
                'product_image': 20480,
                'returned_replaced': 3036,
                'returned_replaced_quantity': 3036,
                'reverse_pickup_reason': 3036,
                'reverse_cancellation_reason': 3036,
                'linked_sku_id': 3036,
                'brand_name': 3036,
                'cart_id': 3036,
                'variant_name': 65535,
                'order_item_state': 3036,
                'slug': 3036,
            },
            'grace_updation_lag': {
                'hours': 4
            },
            'buffer_updation_lag':{
                'hours': 2,
            }
        },
    ]
}