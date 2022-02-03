import awswrangler as wr
from helper.logger import log_writer

def save_to_s3(processed_collection, db_source, db_destination):
    s3_location = "s3://" + db_destination['s3_bucket_name'] + "/" + db_source['source_type'] + "/" + db_source['db_name'] + "/"
    file_name = s3_location + processed_collection['collection_name'] + "/"
    
    log_writer("Attempting to insert at " + file_name)
    try:
        if(processed_collection['df_insert'].shape[0] > 0):
            wr.s3.to_parquet(
                df = processed_collection['df_insert'],
                path = file_name,
                compression='snappy',
                dataset = True,
                partition_cols = ['parquet_format_date_year', 'parquet_format_date_month']
            )
        log_writer("Inserted " + str(processed_collection['df_insert'].shape[0]) + " records at " + file_name)
    except:
        log_writer("Caught some exception while trying to insert records at " + file_name)
    
    log_writer("Attempting to update records at " + file_name)    
    try:
        for i in range(processed_collection['df_update'].shape[0]):
            file_name_u = s3_location + processed_collection['collection_name'] + "/" + "parquet_format_date_year=" + processed_collection['df_update'].iloc[i]['parquet_format_date_year'] + "/parquet_format_date_month=" + processed_collection['df_update'].iloc[i]['parquet_format_date_month'] + "/"
            df_to_be_updated = wr.s3.read_parquet(
                path = file_name_u,
                dataset = True
            )
            df_to_be_updated[df_to_be_updated['_id'] == processed_collection['df_update'].iloc[i]['_id']] = processed_collection['df_update'].iloc[i]
            wr.s3.to_parquet(
                df = df_to_be_updated,
                mode = 'overwrite',
                path = file_name_u,
                compression = 'snappy',
                dataset = True,
            )
        log_writer(str(processed_collection['df_update'].shape[0]) + " updations done for " + file_name)
    except:
        log_writer("Caught some exceptions while saving updated records to " + file_name + ". However, some updations might still have been done :)")
