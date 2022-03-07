from pymongo import MongoClient
import certifi
import awswrangler as wr

from dotenv import load_dotenv
load_dotenv()

client = MongoClient('mongodb+srv://manish:KlSh0bX605PY509h@cluster0.ebwdr.mongodb.net/myFirstDatabase?retryWrites=true&w=majority', tlsCAFile=certifi.where())
database_ = client['migration_update_check']
coll = database_['migration_update_check']

def delete_metadata_from_mongo(id: str = None) -> None:
    _ = coll.delete_many({'collection': id})
    _ = coll.delete_many({'last_run_cron_job_for_id': id})

def del_cmdb_tables():
    delete_metadata_from_mongo('cmdb_tables_to_s3_DMS_localities_live')
    delete_metadata_from_mongo('cmdb_tables_to_s3_DMS_cmocx_cl_in_vicinity')
    delete_metadata_from_mongo('cmdb_tables_to_s3_DMS_inventory_snapshot_wms_query')

def del_support_service():
    delete_metadata_from_mongo('mongo_support_service_to_s3_DMS_leader_kyc')
    delete_metadata_from_mongo('mongo_support_service_to_s3_DMS_support_list')
    delete_metadata_from_mongo('mongo_support_service_to_s3_DMS_support_items')
    delete_metadata_from_mongo('mongo_support_service_to_s3_DMS_support_form_items')
    delete_metadata_from_mongo('mongo_support_service_to_s3_DMS_support_tickets_rating')
    delete_metadata_from_mongo('mongo_support_service_to_s3_DMS_support_ticket_conversations')
    delete_metadata_from_mongo('mongo_support_service_to_s3_DMS_support_tickets')
    delete_metadata_from_mongo('mongo_support_service_to_s3_DMS_webhook_error_logs')

def del_rohan_jobs():
    delete_metadata_from_mongo('cm_audit_logs_to_metabase_s3_DMS_audit_logs')
    delete_metadata_from_mongo('cm_audit_logs_to_metabase_s3_DMS_product_audit_logs')

def see_s3_data(location: str = None):
    df = wr.s3.read_parquet(path=location, dataset=True, path_suffix='.parquet', ignore_empty=True)
    print(df.shape)
    print(df.columns.tolist())
    print(df.dtypes)

del_support_service