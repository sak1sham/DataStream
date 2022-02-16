import os
from dotenv import load_dotenv
load_dotenv()

query_1 = '''
with all_products as (
      select 
          p.sku_id, c.warehouse_name, p.product_name_en as product_name, p.product_status, p.fulfillment_model
      from products p 
      left join catalogues c on 
      c.catalogue_name = p.catalogue_name
      where p.product_status not in ('DELETED') 
      group by 1,2,3,4,5
  ), cte as (
    select 
        piw.warehouse_name, 
        piw.sku_id,
        round((sum((cp * (quantity)::numeric)) / (sum(quantity))::numeric), 2) as cost_price,
        sum(cp*quantity) as inventory_value
    from product_inventory_wms piw
    where piw.quantity > 0 and piw.area_name in ('BULK', 'SELLABLE', 'MRP_CHANGE', 'VARIANT_CHANGE', 'OFFER_CHANGE', 'BLOCKED')
    group by 1,2
    ), inventory as (
    select 
        cte.warehouse_name, 
        cte.sku_id,
        pm.product_name_en as product_name,
        coalesce(bulk.sum, 0) as bulk_quantity,
        coalesce(sellable.sum, 0) as sellable_quantity,
        coalesce(mrp_change.sum, 0) as mrp_change_quantity,
        coalesce(variant_change.sum, 0) as variant_change_quantity,
        coalesce(offer_change.sum, 0) as offer_change_quantity,
        coalesce(blocked.sum, 0) as blocked_quantity,
        cte.cost_price,
        cte.inventory_value,
        pm.product_status,
        pm.fulfillment_model
    from cte 
    left join (
        select warehouse_name, sku_id, sum(quantity) from 
        product_inventory_wms
        where area_name = 'BULK' and quantity > 0
        group by warehouse_name, sku_id
    ) bulk on bulk.warehouse_name = cte.warehouse_name and bulk.sku_id = cte.sku_id
    left join (
        select warehouse_name, sku_id, sum(quantity) from 
        product_inventory_wms
        where area_name = 'SELLABLE' and quantity > 0
        group by warehouse_name, sku_id
    ) sellable on sellable.warehouse_name = cte.warehouse_name and sellable.sku_id = cte.sku_id
    left join (
        select warehouse_name, sku_id, sum(quantity) from 
        product_inventory_wms
        where area_name = 'MRP_CHANGE' and quantity > 0
        group by warehouse_name, sku_id
    ) mrp_change on mrp_change.warehouse_name = cte.warehouse_name and mrp_change.sku_id = cte.sku_id
    left join (
        select warehouse_name, sku_id, sum(quantity) from 
        product_inventory_wms
        where area_name = 'VARIANT_CHANGE' and quantity > 0
        group by warehouse_name, sku_id
    ) variant_change on variant_change.warehouse_name = cte.warehouse_name and variant_change.sku_id = cte.sku_id
    left join (
        select warehouse_name, sku_id, sum(quantity) from 
        product_inventory_wms
        where area_name = 'OFFER_CHANGE' and quantity > 0
        group by warehouse_name, sku_id
    ) offer_change on offer_change.warehouse_name = cte.warehouse_name and offer_change.sku_id = cte.sku_id
    left join (
        select warehouse_name, sku_id, sum(quantity) from 
        product_inventory_wms
        where area_name = 'BLOCKED' and quantity > 0
        group by warehouse_name, sku_id
    ) blocked on blocked.warehouse_name = cte.warehouse_name and blocked.sku_id = cte.sku_id
    left join product_master pm on pm.sku_id = cte.sku_id and pm.catalogue_name = 'DELHI_NCR'
  ) select 
    ap.warehouse_name, 
    ap.sku_id, 
    ap.product_name,
    coalesce(i.bulk_quantity, 0) as bulk_quantity,
    coalesce(i.sellable_quantity, 0) as sellable_quantity,
    coalesce(i.mrp_change_quantity, 0) as mrp_change_quantity,
    coalesce(i.variant_change_quantity, 0) as variant_change_quantity,
    coalesce(i.offer_change_quantity, 0) as offer_change_quantity,
    coalesce(i.blocked_quantity, 0) as blocked_quantity,
    coalesce(i.cost_price, 0) as cost_price,
    coalesce(i.inventory_value, 0) inventory_value,
    ap.product_status,
    ap.fulfillment_model 
  from all_products ap left join inventory i on i.warehouse_name = ap.warehouse_name and i.sku_id = ap.sku_id

'''

mapping = [
    {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'cm',
            'password': 'cm'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'data-migration-server'
        },
        'tables':[
            {
                'table_name': 'localities_live',
                'cron': '* * * * * 14 51 0',
                'to_partition': True,
                'partition_col': 'migration_snapshot_date',
                'partition_col_format': 'datetime',
                'is_dump': True
            },
            {
                'table_name': 'inventory_snapshot_wms',
                'cron': '* * * * * 14 51 0',
                'to_partition': True,
                'partition_col': 'migration_snapshot_date',
                'partition_col_format': 'datetime',
                'is_dump': True,
                'fetch_data_query': query_1
            }
        ]
    },
    {
        'source': {
            'source_type': 'mongo',
            'url': 'mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservice.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
            'db_name': 'support-service'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'data-migration-server'
        },
        'collections': [
            {
                'collection_name': 'leader_kyc',
                'fields': {},
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 14 51 0',
                'to_partition': True
            },
            {
                'collection_name': 'support_form_items',
                'fields': {},
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 14 51 0',
                'to_partition': True,
                'is_dump': True,
                'partition_col': 'migration_snapshot_date'
            },
            {
                'collection_name': 'support_items',
                'fields': {
                    'priority': 'int',
                },
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 15 8 0',
                'to_partition': True
            },
            {
                'collection_name': 'support_list',
                'fields': {},
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 14 51 0',
                'to_partition': True
            },
            {
                'collection_name': 'support_ticket_conversations',
                'fields': {
                    'incoming': 'bool',
                    'private': 'bool',
                    'freshdesk_user_id': 'float',
                    '__v': 'float',
                },
                'bookmark': 'updated_at',
                'archive': False,
                'cron': '* * * * * 14 51 0',
                'to_partition': True,
            },
            {
                'collection_name': 'support_tickets',
                'fields': {
                    'created_at': 'datetime',
                    'spam': 'bool',
                    'priority': 'int',
                    'source': 'int',
                    'status': 'int',
                    'is_escalated': 'bool',
                    'updated_at': 'datetime',
                    'nr_escalated': 'bool',
                    'fr_escalated': 'bool',
                    '__v': 'int',
                },
                'bookmark': 'updated_at',
                'archive': False,
                'cron': '* * * * * 11 32 50',
                'to_partition': True,
            },
            {
                'collection_name': 'support_tickets_rating',
                'fields': {
                    'rating': 'int',
                    '__v': 'int',
                },
                'bookmark': 'updatedAt',
                'archive': False,
                'cron': '* * * * * 14 51 0',
                'to_partition': True,
            },
            {
                'collection_name': 'webhook_error_logs',
                'fields': {},
                'bookmark': False,
                'archive': False,
                'cron': '* * * * * 14 51 0',
                'to_partition': True
            },
        ]
    }
]

encryption_store = {
    'url': os.getenv('ENCR_MONGO_URL'),
    'db_name': os.getenv('DB_NAME'),
    'collection_name': os.getenv('COLLECTION_NAME')
}