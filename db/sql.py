from sqlalchemy import create_engine, Table, MetaData, select
import pandas as pd
from helper.logger import log_writer
from dst.s3 import save_to_s3
from pymongo import MongoClient
from helper.util import convert_list_to_string, convert_to_type, convert_to_datetime
import certifi
import datetime
from config.migration_mapping import encryption_store
import hashlib
import pytz

IST_tz = pytz.timezone('Asia/Kolkata')

def get_data_from_encr_db():
    try:
        client_encr = MongoClient(encryption_store['url'], tlsCAFile=certifi.where())
        db_encr = client_encr[encryption_store['db_name']]
        collection_encr = db_encr[encryption_store['collection_name']]
        log_writer("Successfully connected to encryption database.")
        return collection_encr
    except:
        log_writer("Unable to connect to encryption store database.")
        return None

def distribute_records(collection_encr, df, table_unique_id):
    df = df.sort_index(axis = 1)
    df_insert = pd.DataFrame({})
    df_update = pd.DataFrame({})
    for i in range(df.shape[0]):
        encr = {
            'table': table_unique_id,
            'map_id': df.iloc[i].unique_migration_record_id,
            'record_sha': hashlib.sha256(convert_list_to_string(df.loc[i, :].values.tolist()).encode()).hexdigest()
        }
        previous_records = collection_encr.find_one({'table': table_unique_id, 'map_id': df.iloc[i].unique_migration_record_id})
        if(previous_records):
            if(previous_records['record_sha'] == encr['record_sha']):
                continue
            else:
                df_update = df_update.append(df.loc[i, :])
                collection_encr.delete_one({'table': table_unique_id, 'map_id': df.iloc[i].unique_migration_record_id})
                collection_encr.insert_one(encr)
        else:
            df_insert = df_insert.append(df.loc[i, :])
            collection_encr.insert_one(encr)
    return df_insert, df_update

def filter_df(df, table_mapping={}):
    log_writer("Total " + str(df.shape[0]) + " records present in table.")

    ## Fetching encryption database
    ## Encryption database is used to store hashes of records in case bookmark is absent
    collection_encr = get_data_from_encr_db()
    if(collection_encr is None):
        return None, None

    if('last_run_cron_job' not in table_mapping.keys()):
        table_mapping['last_run_cron_job'] = IST_tz.localize(datetime.datetime(1602, 8, 20, 0, 0, 0, 0))

    if('to_partition' in table_mapping.keys() and table_mapping['to_partition']):
        if('partition_col_format' not in table_mapping.keys() or not table_mapping['partition_col_format']):
            df['parquet_format_date_year'] = df[table_mapping['partition_col']].year
            df['parquet_format_date_month'] = df[table_mapping['partition_col']].month
        else:
            df['parquet_format_date_year'] = df[table_mapping['partition_col']].apply(lambda x: convert_to_datetime(x, table_mapping['partition_col_format'])).year
            df['parquet_format_date_month'] = df[table_mapping['partition_col']].apply(lambda x: convert_to_datetime(x, table_mapping['partition_col_format'])).month

    df_consider = df
    if(table_mapping['bookmark']):
        # Use bookmark for comparison of updation time
        if('bookmark_format' not in table_mapping.keys()):
            df_consider = df[df[table_mapping['bookmark']] > table_mapping['last_run_cron_job']]
        else:
            df_consider = df[df[table_mapping['bookmark']].apply(lambda x: convert_to_datetime(x, table_mapping['bookmark_format'])) > table_mapping['last_run_cron_job']]
    
    table_mapping['last_run_cron_job'] = datetime.datetime.utcnow().replace(tzinfo = IST_tz)
    
    if(isinstance(table_mapping['uniqueness'], str)):
        table_mapping['uniqueness'] = [table_mapping['uniqueness']]
    df_consider['unique_migration_record_id'] = df_consider[table_mapping['uniqueness']].astype(str).sum(1)

    df_insert, df_update = distribute_records(collection_encr, df_consider, table_mapping['table_unique_id'])
    
    log_writer("All records processed.")
    log_writer("Insertions: " + str(df_insert.shape[0]))        
    log_writer("Updations: " + str(df_update.shape[0]))        
    return df_insert, df_update


def get_data(db, table_name):
    try:
        engine = create_engine(db['source']['url'])
        metadata = MetaData()
        table = Table(table_name, metadata, autoload=True, autoload_with=engine)
        stmt = select([table])
        df = pd.read_sql(stmt, engine)
        return df
    except:
        log_writer("Unable to connect sql:" + db['source']['db_name'] + ":" + table_name)
        return None

def process_data(df, table):
    try:
        df_insert, df_update = filter_df(df=df, table_mapping=table)
        if(df_insert is not None and df_update is not None):
            return {'name': table['table_name'], 'df_insert': df_insert, 'df_update': df_update}
        else:
            return None
    except:
        log_writer("Caught some exception while processing " + table['table_unique_id'])
        return None

def save_data(db, processed_table):
    if(db['destination']['destination_type'] == 's3'):
        save_to_s3(processed_table, db_source=db['source'], db_destination=db['destination'])

def process_sql_table(db, table):
    df = get_data(db, table['table_name'])
    if(df is not None):
        log_writer('Fetched data for ' + table['table_unique_id'])
        processed_table = process_data(df=df, table=table)
        if(processed_table is not None):
            log_writer('Processed data for ' + table['table_unique_id'])
            try:
                save_data(db=db, processed_table=processed_table)
                log_writer('Successfully saved data for ' + table['table_unique_id'])
            except:
                log_writer('Caught some exception while saving data from ' + table['table_unique_id'])
