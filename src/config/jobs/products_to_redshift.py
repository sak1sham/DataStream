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
            'table_name': 'products',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at_for_pipeline',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'lob_fields_length': {
                'jit_source': 65535,
                'warnings': 65535,
                'delivery_days': 65535,
                'pday_to_dday': 65535,
                'properties': 65535,

                'keywords': 3036,
                'economic_segment': 3036,
                'storage_type': 3036,
                'vendor_product_id': 3036,
                'product_image': 3036,
                'variant_for_search': 3036,
                'brand_name_hi': 3036,
                'variant_identifier_hi': 3036,
                'catalogue_name': 3036,
                'slug': 3036,
                'product_description_hi': 10240,
                'product_description_en': 10240,
                'brand_discount_type': 3036,
                'product_tag': 3036,
                'linked_sku_id': 3036,
                'cm_discount_type': 3036,
                'variant_identifier': 3036,
                'temperature_requirements': 3036,
                'storage_instructions': 3036,
                'allowed_payment_method': 3036,
                'product_description_md_hi': 10240,
                'product_policy': 3036,
                'item_size': 3036,
                'brand_categorisation': 3036,
                'product_description_md_en': 3036,
                'product_status': 3036,
                'variant_identifier2': 3036,
                'discount_type': 3036,
                'price_per_unit': 3036,
                'source': 3036,
                'variant_priority2': 3036,
                'sku_id': 3036,
                'variant_grammage_unit': 3036,
                'error': 20600,
                'error_log': 3036,
                'fulfillment_type': 3036,
                'fulfillment_model': 3036,
                'variant_grammage': 3036,
                'variant2_name': 3036,
                'variant_size': 3036,
                'variant_color': 3036,
                'variant_fabric_composition': 3036,
                'ean_code': 3036,
                'unit_for_ppu': 3036,
                'custom_symbol': 3036,
                'pack_type': 3036,
                'brand_name': 3036,
                'marketer': 3036,
                'custom_text': 3036,
                'hsn_code': 3036,

                'short_name_hi': 3036,
                'product_name_en': 3036,
                'product_name_hi': 3036,
                'short_name_en': 3036,
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
