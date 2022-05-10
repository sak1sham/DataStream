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
            'table_name': 'cms_items',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'cms_item_id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'col_rename': {
                'tag': 'tag_'
            },
            'lob_fields_length': {
                'sub_category': 65535,
                'original_images': 65535,
                'disallowed_user_tags': 65535,
                'background_color': 65535,
                'catalogues': 65535,
                'critical_skus': 65535,
                'critical_coupons': 65535,
                'tags': 65535,
                'super_category': 65535,
                'primary_category': 65535,
                'ptype': 65535,
                'type': 65535,
                'query_params': 65535,

                'offer_type': 3036,
                'title_ref': 3036,
                'image_link': 3036,
                'cx_sharing_template_id': 3036,
                'cl_sharing_template_id': 3036,
                'cx_title': 3036,
                'cl_title': 3036,
                'sku_id': 3036,
                'sku_slug': 3036,
                'sku_short_name_hi': 3036,
                'product_query_id': 3036,
                'link': 3036,
                'link_params': 3036,
                'link_type': 3036,
                'tag': 3036,
                'sub_type': 3036,
                'video_link': 3036,
                'text_ctx_cx': 3036,
                'text_ctx_cl': 3036,
                'error': 3036,
                'warning': 3036,
                'app_name': 3036,
                'overlay_template_name': 3036,
                'cx_subtitle': 3036,
                'cl_subtitle': 3036,
                'poll_id': 3036,
                'banner_type': 3036,
                'campaign_name': 3036,
                'category_entity_id': 3036,
                'pdu_type': 3036,

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