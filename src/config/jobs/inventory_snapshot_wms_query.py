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

mapping = {
    'source': {
        'source_type': 'sql',
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'cmdb',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
    },
    'destination': { 
        'destination_type': 's3', 
        'specifications': [
            {
                's3_bucket_name': 'database-migration-service-prod' 
            }
        ]
    }, 
    'tables': [
        {
            'table_name': 'inventory_snapshot_wms_query',
            'cron': 'self-managed',
            'to_partition': True,
            'partition_col': 'migration_snapshot_date',
            'partition_col_format': 'datetime',
            'mode': 'dumping',
            'fetch_data_query': query_1,
            'fields': {
                'bulk_quantity': 'int',
                'sellable_quantity': 'int',
                'mrp_change_quantity': 'int',
                'variant_change_quantity': 'int',
                'offer_change_quantity': 'int',
                'blocked_quantity': 'int',
                'cost_price': 'float',
                'inventory_value': 'float',
            },
            'batch_size': 10000,
        },
    ]
}
