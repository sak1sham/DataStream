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

'''
mapping = {
    "dms_iswq_ll_cciv": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'database-migration-service-prod'
        },
        'tables': [
            {
                'table_name': 'inventory_snapshot_wms_query',
                'cron': '* * * * * */1 0 0',
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
                }
            },
            {
                'table_name': 'localities_live',
                'cron': '* * * * * 22 10 0',
                'to_partition': True,
                'partition_col': 'migration_snapshot_date',
                'partition_col_format': 'datetime',
                'mode': 'dumping',
            },
            {
                'table_name': 'cmocx_cl_in_vicinity',
                'cron': '* * * * * 22 10 0',
                'to_partition': True,
                'partition_col': 'migration_snapshot_date',
                'partition_col_format': 'datetime',
                'mode': 'dumping',
            }
        ]
    },
    "mongo_s3_support": {
        'source': {
            'source_type': 'mongo',
            'url': 'mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservicev2.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
            'db_name': 'support-service'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'data-migration-server',
        },
        'collections': [
            {
                'collection_name': 'support_ticket_conversations',
                'fields': {
                    'incoming': 'bool',
                    'private': 'bool',
                    'freshdesk_user_id': 'int',
                    '__v': 'int',
                    'created_at': 'datetime',
                    'updated_at': 'datetime',
                    'created_ts': 'datetime',
                    'updated_ts': 'datetime'
                },
                'bookmark': 'updated_ts',
                'archive': False,
                'cron': '* * * * * 17 28 0',
                'to_partition': True,
                'mode': 'syncing',
                'improper_bookmarks': False
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
                    'agent_responded_at': 'datetime',
                    'cancelled_at':'datetime',
                    'closed_at':'datetime',
                    'due_by':'datetime',
                    'first_responded_at':'datetime',
                    'fr_due_by':'datetime',
                    'resolved_at':'datetime',
                    'status_updated_at':'datetime',
                    'created_ts': 'datetime',
                    'updated_ts': 'datetime',
                },
                'bookmark': 'updated_ts',
                'archive': False,
                'cron': '* * * * * 17 28 0',
                'to_partition': True,
                'mode': 'syncing',
                'improper_bookmarks': False
            }
        ]
    },
}
'''

'''
mapping = {
    "order_actions_cmdb_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'database-migration-service-prod'
        },
        'tables': [
            {
                'table_name': 'order_actions',
                'cron': '2022 3 4 * * 10 32 0',
                'to_partition': True,
                'bookmark_creation': 'created_at',
                'bookmark': 'created_at',
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
            }
        ]
    },
    "inventory_transactions_wms_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'wmsdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'database-migration-service-prod'
        },
        'tables': [
            {
                'table_name': 'inventory_transactions',
                'cron': '2022 3 4 * * 10 32 0',
                'to_partition': True,
                'bookmark_creation': 'created_at',
                'bookmark': 'created_at',
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
            }
        ]
    },
}

mapping = {
    "habitual_users_cmdb_redshift": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 'redshift',
            'host': 'redshift-cluster-1.cyl4ilkelm5m.ap-south-1.redshift.amazonaws.com',
            'database': 'dev',
            'user': 'admin-redshift',
            'password': 'CitymallDevAdmin123',
            'schema': 'migration_service',
            's3_bucket_name': 'database-migration-service-prod',
        },
        'tables': [
            {
                'table_name': 'analytics.habitual_users',
                'cron': 'self-managed',
                'to_partition': True
            },
        ]
    },
}

#'''

'''
mapping = {
    "Rohan_audit_logs": {
        'source': {
            'source_type': 'mongo',
            'url': 'mongodb://cm-audit-logs:d1TCvFEVX4UbwuuYlM9EwJlkhV2K4NdWRyKASYn4cwj87157zUv73IGE85YAh2DsVJO7HrtWNzOvVvwWjn56ww==@cm-audit-logs.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@cm-audit-logs@',
            'db_name': 'test'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'database-migration-service-prod',
        },
        'collections': [
            {
                'collection_name': 'audit_logs',
                'fields': {
                    'user_id_bigint': 'int',
                    'created_at': 'datetime',
                    'lat': 'float',
                    'long': 'float'
                },
                'cron': '2022 3 8 * * 0 6 0',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'created_at',
                'bookmark_creation': 'created_at',
                'batch_size': 200,
                'time_delay': 0.5,
            },
            {
                'collection_name': 'product_audit_logs',
                'fields': {
                    'user_id_bigint': 'int',
                    'created_at': 'datetime',
                    'lat': 'float',
                    'long': 'float'
                },
                'cron': '2022 3 8 * * 0 6 0',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'created_at',
                'bookmark_creation': 'created_at',
                'batch_size': 200,
                'time_delay': 0.5,
            }
        ]
    },
    "Rohan_notifications_cmdb" : {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'database-migration-service-prod'
        },
        'tables': [
            {
                'table_name': 'notifications',
                'cron': '2022 3 8 * * 0 6 0',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'created_at',
                'bookmark_creation': 'created_at'
            }
        ]
    },
}
#'''

'''
mapping = {
    "impression_service": {
        'source': {
            'source_type': 's3',
            'url': 's3://app-impression-go/',
            'db_name': 'dms',
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'database-migration-service-prod'
        },
        'tables': [
            {
                'table_name': 'impression_service',
                'cron': '* * * * * 16 10 0',
                'to_partition': True,
                'partition_col': 'insertion_date',
                'partition_col_format': 'datetime',
                'bookmark': 'insertion_date',
                'bookmark_creation': 'insertion_date',
                'fields': {
                    'screen_name': 'str',
                    'user_id': 'str',
                    'ct_profile_id': 'str',
                    'asset_id': 'int',
                    'asset_type': 'str',
                    'asset_parent_id': 'str',
                    'asset_parent_type': 'str',
                    'price': 'int',
                    'mrp': 'int',
                    'action': 'str',
                    'app_type': 'str',
                    'date': 'datetime',
                    'entity_type': 'str',
                    'vertical_rank': 'int',
                    'horizontal_rank': 'int',
                    'source': 'str',
                    'is_product_oos': 'bool',
                    'catalogue_name': 'str',
                    'insertion_date': 'datetime',
                    'cms_page_id': 'str',
                    'linked_cms': 'str',
                    'linked_cat': 'str',
                    'linked_subcat': 'str',
                },
            }
        ]
    }
}
#'''

mapping = {
    "ct-cx-app-events-to-redshift": {
        "source": {
            "source_type": "api",
            "db_name": "clevertap"
        },
        "destination": {
            'destination_type': 'redshift',
            'host': 'redshift-cluster-1.cyl4ilkelm5m.ap-south-1.redshift.amazonaws.com',
            'database': 'dev',
            'user': 'admin-redshift',
            'password': 'CitymallDevAdmin123',
            'schema': 'cm_clevertap',
            's3_bucket_name': 'database-migration-service-prod',
        },
        "apis": [
            {
                'api_name':'cx_app_events',
                'project_name': 'cx_app',
                'event_names': '*',
                'bookmark_key_type': 'date',
                'bookmark_key_format': 'YYYYMMDD',
                'bookmark_key': '-1',
                'slack_channel':'C025PTAUUFP',
                'fields': {
                    "event_name": 'str',
                    "ct_ts": 'int',
                    "timestamp": 'datetime',
                    "name": 'str',
                    "phone": 'str',
                    "cx_city": 'str',
                    "city": 'str',
                    "user_id": 'str',
                    "whatsapp_opted_in": 'str',
                    "leader_id": 'str',
                    "leader_name": 'str',
                    "leader_user_id": 'str',
                    "leader_lat":'str',
                    "leader_lng": 'str',
                    "catalogue_name": 'str',
                    "platform": 'str',
                    "ct_object_id": 'str',
                    "ct_session_id": 'str',
                    "screen_name": 'str',
                    "os_version": 'str',
                    "app_version": 'str',
                    "make": 'str',
                    "model": 'str',
                    "cplabel": 'str',
                    "tags": 'str',
                    "event_props": 'str'
                },
                'lob_fields': {
                    "tags": 4096,
                    "event_props": 4096
                },
                'api_to_field_mapping': {
                    "ct_ts": 'ts',
                    "name": 'profile.name',
                    "phone": 'profile.phone',
                    "cx_city": 'profile.profileData.cx_city',
                    "city": 'profile.profileData.city',
                    "user_id": 'profile.profileData.user_id',
                    "whatsapp_opted_in": 'profile.profileData.whatsapp_opted_in',
                    "leader_id": 'profile.profileData.leaderid',
                    "leader_name": 'profile.profileData.leadername',
                    "leader_user_id": 'profile.profileData.leaderuserid',
                    "leader_lat":'profile.profileData.leaderlat',
                    "leader_lng": 'profile.profileData.leaderlng',
                    "catalogue_name": 'profile.profileData.catalogue_name',
                    "platform": 'profile.platform',
                    "ct_object_id": 'profile.objectId',
                    "ct_session_id": 'event_props.CT Session Id',
                    "screen_name": 'event_props.screen_name',
                    "os_version": 'profile.os_version',
                    "app_version": 'profile.app_version',
                    "make": 'profile.make',
                    "model": 'profile.model',
                    "cplabel": 'profile.profileData.cplabel',
                    "tags": 'profile.profileData.tags',
                    "event_props": 'event_props'
                },
                'cron': 'self-managed',
            }
        ]
    },
    "ct-cl-app-events-to-redshift": {
        "source": {
            "source_type": "api",
            "db_name": "clevertap"
        },
        "destination": {
            'destination_type': 'redshift',
            'host': 'redshift-cluster-1.cyl4ilkelm5m.ap-south-1.redshift.amazonaws.com',
            'database': 'dev',
            'user': 'admin-redshift',
            'password': 'CitymallDevAdmin123',
            'schema': 'cm_clevertap',
            's3_bucket_name': 'database-migration-service-prod',
        },
        "apis": [
            {
                'api_name':'cl_app_events',
                'project_name': 'cl_app',
                'event_names': '*',
                'bookmark_key_type': 'date',
                'bookmark_key_format': 'YYYYMMDD',
                'bookmark_key': '-1',
                'slack_channel':'C025PTAUUFP',
                'fields': {
                    "event_name": 'str',
                    "ct_ts": 'int',
                    "timestamp": 'datetime',
                    "name": 'str',
                    "phone": 'str',
                    "cx_city": 'str',
                    "city": 'str',
                    "user_id": 'str',
                    "whatsapp_opted_in": 'str',
                    "leader_id": 'str',
                    "leader_name": 'str',
                    "leader_user_id": 'str',
                    "leader_lat":'str',
                    "leader_lng": 'str',
                    "catalogue_name": 'str',
                    "platform": 'str',
                    "ct_object_id": 'str',
                    "ct_session_id": 'str',
                    "screen_name": 'str',
                    "os_version": 'str',
                    "app_version": 'str',
                    "make": 'str',
                    "model": 'str',
                    "cplabel": 'str',
                    "tags": 'str',
                    "event_props": 'str'
                },
                'lob_fields': {
                    "tags": 4096,
                    "event_props": 4096
                },
                'api_to_field_mapping': {
                    "ct_ts": 'ts',
                    "name": 'profile.name',
                    "phone": 'profile.phone',
                    "cx_city": 'profile.profileData.cx_city',
                    "city": 'profile.profileData.city',
                    "user_id": 'profile.profileData.user_id',
                    "whatsapp_opted_in": 'profile.profileData.whatsapp_opted_in',
                    "leader_id": 'profile.profileData.leaderid',
                    "leader_name": 'profile.profileData.leadername',
                    "leader_user_id": 'profile.profileData.leaderuserid',
                    "leader_lat":'profile.profileData.leaderlat',
                    "leader_lng": 'profile.profileData.leaderlng',
                    "catalogue_name": 'profile.profileData.catalogue_name',
                    "platform": 'profile.platform',
                    "ct_object_id": 'profile.objectId',
                    "ct_session_id": 'event_props.CT Session Id',
                    "screen_name": 'event_props.screen_name',
                    "os_version": 'profile.os_version',
                    "app_version": 'profile.app_version',
                    "make": 'profile.make',
                    "model": 'profile.model',
                    "cplabel": 'profile.profileData.cplabel',
                    "tags": 'profile.profileData.tags',
                    "event_props": 'event_props'
                },
                'cron': 'self-managed',
            }
        ]
    },
    "ct-cx-web-events-to-redshift": {
        "source": {
            "source_type": "api",
            "db_name": "clevertap"
        },
        "destination": {
            'destination_type': 'redshift',
            'host': 'redshift-cluster-1.cyl4ilkelm5m.ap-south-1.redshift.amazonaws.com',
            'database': 'dev',
            'user': 'admin-redshift',
            'password': 'CitymallDevAdmin123',
            'schema': 'cm_clevertap',
            's3_bucket_name': 'database-migration-service-prod',
        },
        "apis": [
            {
                'api_name':'cx_web_events',
                'project_name': 'cx_web',
                'event_names': '*',
                'bookmark_key_type': 'date',
                'bookmark_key_format': 'YYYYMMDD',
                'bookmark_key': '-1',
                'slack_channel':'C025PTAUUFP',
                'fields': {
                    "event_name": 'str',
                    "ct_ts": 'int',
                    "timestamp": 'datetime',
                    "session_source": 'str',
                    "visitor_id": 'str',
                    "slug": 'str',
                    "event_props": 'str'
                },
                'lob_fields': {
                    "slug": 1024,
                    "event_props": 4096
                },
                'api_to_field_mapping': {
                    "ct_ts": 'ts',
                    "session_source": 'session_props.session_source',
                    "visitor_id": 'event_props.visitorId',
                    "slug": 'event_props.slug',
                    "event_props": 'event_props'
                },
                'cron': 'self-managed',
            }
        ]
    }
}

settings = {
    'fastapi_server': True,
    'timezone': 'Asia/Kolkata',
    'notify': True,
    'encryption_store': {
        'url': os.getenv('ENCR_MONGO_URL'),
        'db_name': os.getenv('DB_NAME'),
        'collection_name': os.getenv('COLLECTION_NAME')
    },
}
#'''

'''
mapping = {
    "test_modes_pgsql": {
        'source': {
            'source_type': 'sql',
            'url': 'localhost',
            'db_name': 'postgres',
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'data-migration-server'
        },
        'tables': [
            {
                'table_name': 'accounts',
                'cron': 'self-managed',
                'mode': 'logging',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                'to_partition': True,
            }
        ]
    },
}
#'''

'''
mapping = {
    'testing_only': {
        'source': {
            'source_type': 'mongo',
            'url': 'mongodb+srv://manish:KlSh0bX605PY509h@cluster0.ebwdr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
            'db_name': 'test'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'learning-migrationservice',
        },
        'collections': [
            {
                'collection_name': 'migration_test',
                'fields': {
                    'Age': 'int',
                    'Marks': 'int',
                    'Updated_at': 'datetime',
                    'Random_other': 'int',
                },
                'bookmark': 'Updated_at',
                'archive': False,
                'cron': 'self-managed',
                'to_partition': True,
                'mode': 'syncing',
                'improper_bookmarks': True
            },
        ]
    }
}
#'''

'''
mapping = {
    'testing_only': {
        'source': {
            'source_type': 'kafka',
            'kafka_server': 'localhost:9092',
            'db_name': 'test'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'learning-migrationservice',
        },
        'topics': [
            {
                'topic_name': 'test_2',
                'fields': {
                    'Name': 'str',
                    'Marks': 'int',
                    'Class': 'int',
                },
                'cron': 'self-managed',
                'to_partition': True,
                'partition_col': 'Class',
                'partition_col_format': ['int']
            },
        ]
    }
}
#'''

'''
mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [
            {
                'table_name': 'tbl_user',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'user_id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'user_created',
                'partition_col_format': 'datetime',
                'bookmark': 'user_updated',
                'improper_bookmarks': False
            },
        ]
    }
}'''


'''
mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [          
            {
                'table_name': 'user_addresses',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [
            {
                'table_name': 'team_leader_users',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [
            {
                'table_name': 'team_leaders',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'str',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [
            {
                'table_name': 'products',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [
            {
                'table_name': 'user_cashbacks',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'str',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [
            {
                'table_name': 'product_categories',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                # 'to_partition': True,
                # 'partition_col': 'created_at',
                # 'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [            
            {
                'table_name': 'cms_list_items',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'cms_list_item_id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [            
            {
                'table_name': 'cms_items',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'cms_item_id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [
            {
                'table_name': 'pincodes',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                # 'to_partition': True,
                # 'partition_col': 'created_at',
                # 'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [
            {
                'table_name': 'categories',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'category_id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [
            {
                'table_name': 'tbl_admin',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'admin_id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'admin_created',
                'partition_col_format': 'datetime',
                'bookmark': 'admin_updated',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [
            {
                'table_name': 'batch_process_table',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [            
            {
                'table_name': 'csv_batch_uploads',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'batch_id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [
            {
                'table_name': 'team_leader_sessions',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'str',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [            
            {
                'table_name': 'rm_numbers',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [            
            {
                'table_name': 'commission_errors',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                # 'bookmark': 'updated_at',
                # 'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [            
            {
                'table_name': 'idfas_to_ban_usage',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                # 'bookmark': 'updated_at',
                # 'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [            
            {
                'table_name': 'db_cash_transition_ledger',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [            
            {
                'table_name': 'spoke_cash_transition_ledger',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [            
            {
                'table_name': 'analytics.cl_idfas',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'cl_id',
                'primary_key_datatype': 'str',
                # 'to_partition': True,
                # 'partition_col': 'created_at',
                # 'partition_col_format': 'datetime',
                # 'bookmark': 'updated_at',
                # 'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [            
            {
                'table_name': 'analytics.cl_funnel',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'cl_id',
                'primary_key_datatype': 'str',
                'to_partition': True,
                'partition_col': 'cl_signup_date',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [            
            {
                'table_name': 'order_item_fulfillment_model',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
        },
        'tables': [            
            {
                'table_name': 'order_items',
                'cron': 'self-managed',
                'mode': 'syncing',
                'primary_key': 'id',
                'primary_key_datatype': 'int',
                'to_partition': True,
                'partition_col': 'created_at',
                'partition_col_format': 'datetime',
                'bookmark': 'updated_at',
                'improper_bookmarks': False
            },
        ]
    }
}



mapping = {
    "from_cmdb_to_s3": {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp'
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
                'improper_bookmarks': False
            },
        ]
    },
}
'''

settings = {
    'fastapi_server': True,
    'timezone': 'Asia/Kolkata',
    'notify': False,
    'encryption_store': {
        'url': os.getenv('ENCR_MONGO_URL'),
        'db_name': os.getenv('DB_NAME'),
        'collection_name': os.getenv('COLLECTION_NAME')
    },
    'logging_store': {
        'url': os.getenv('LOG_MONGO_URL'),
        'db_name': os.getenv('LOG_DB_NAME'),
        'collection_name': os.getenv('LOG_COLLECTION_NAME')
    },
    'slack_notif': {
        'slack_token': 'xoxb-667683339585-3192552509475-C0xJXwmmUUwrIe4FYA0pxv2N',
        'channel': "C035WQHD291"
    },
    'save_logs': False
}
