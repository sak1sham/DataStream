from sqlalchemy import create_engine, Table, MetaData, select
import pandas as pd
import logging
logging.getLogger().setLevel(logging.INFO)

from dst.s3 import save_to_s3
from helper.util import convert_list_to_string, convert_to_datetime
import datetime
import hashlib
import pytz
from db.encr_db import get_data_from_encr_db, get_last_run_cron_job
import psycopg2
import pandas.io.sql as sqlio

IST_tz = pytz.timezone('Asia/Kolkata')

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
    logging.info(table_mapping['table_unique_id'] + ": Total " + str(df.shape[0]) + " records present.")

    collection_encr = get_data_from_encr_db()
    last_run_cron_job = table_mapping['last_run_cron_job']
    
    if('is_dump' in table_mapping.keys() and table_mapping['is_dump']):
        df['migration_snapshot_date'] = datetime.datetime.utcnow().replace(tzinfo = IST_tz)
    
    table_mapping['partition_for_parquet'] = []
    if('to_partition' in table_mapping.keys() and table_mapping['to_partition']):
        if('partition_col' in table_mapping.keys()):
            if(isinstance(table_mapping['partition_col'], str)):
                table_mapping['partition_col'] = [table_mapping['partition_col']]
            if('partition_col_format' not in table_mapping.keys()):
                table_mapping['partition_col_format'] = ['str']
            if(isinstance(table_mapping['partition_col_format'], str)):
                table_mapping['partition_col_format'] = [table_mapping['partition_col_format']]
            while(len(table_mapping['partition_col']) > len(table_mapping['partition_col_format'])):
                table_mapping['partition_col_format'] = table_mapping['partition_col_format'].append('str')
            # Now, there is a 1-1 mapping of partition_col and partition_col_format. Now, we need to partition.
            
            for i in range(len(table_mapping['partition_col'])):
                col = table_mapping['partition_col'][i].lower()
                col_form = table_mapping['partition_col_format'][i]
                parq_col = "parquet_format_" + col
                
                if(col == 'migration_snapshot_date'):
                    col_form = 'datetime'
                    table_mapping['partition_for_parquet'].extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                    temp = df[col].apply(lambda x: convert_to_datetime(x))
                    df[parq_col + "_year"] = temp.dt.year
                    df[parq_col + "_month"] = temp.dt.month
                    df[parq_col + "_day"] = temp.dt.day
                elif(col_form == 'str'):
                    table_mapping['partition_for_parquet'].extend([parq_col])
                    df[parq_col] = df[col].astype(str)
                elif(col_form == 'int'):
                    table_mapping['partition_for_parquet'].extend([parq_col])
                    df[parq_col] = df[col].astype(int)
                elif(col_form == 'datetime'):
                    table_mapping['partition_for_parquet'].extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                    temp = df[col].apply(lambda x: convert_to_datetime(x))
                    df[parq_col + "_year"] = temp.dt.year
                    df[parq_col + "_month"] = temp.dt.month
                    df[parq_col + "_day"] = temp.dt.day
                else:
                    logging.error(table_mapping['table_unique_id'] + ": Parition_col_format wrongly specified.")
                    return None, None
        else:
            logging.warning(table_mapping['table_unique_id'] + ": Unable to find partition_col. Continuing without partitioning")

    df_consider = df
    df_insert = pd.DataFrame({})
    df_update = pd.DataFrame({})

    if('is_dump' not in table_mapping.keys() or not table_mapping['is_dump']):
        if('primary_keys' not in table_mapping):
            # If unique keys are not specified by user, consider entire rows are unique keys
            table_mapping['primary_keys'] = df.columns.values.tolist()
            logging.warning(str(table_mapping['table_unique_id']) + ": Unable to find primary_keys in mapping. Taking entire records into consideration.")
        if(isinstance(table_mapping['primary_keys'], str)):
            table_mapping['primary_keys'] = [table_mapping['primary_keys']]
        table_mapping['primary_keys'] = [x.lower() for x in table_mapping['primary_keys']]
        df['unique_migration_record_id'] = df[table_mapping['primary_keys']].astype(str).sum(1)

    if('is_dump' not in table_mapping.keys() or not table_mapping['is_dump']):
        if('bookmark' in table_mapping.keys() and table_mapping['bookmark']):
            df_consider = df[df[table_mapping['bookmark']].apply(lambda x: convert_to_datetime(x)) > last_run_cron_job]
            if('bookmark_creation' in table_mapping.keys() and table_mapping['bookmark_creation']):
                df_insert = df_consider[df_consider[table_mapping['bookmark_creation']].apply(lambda x: convert_to_datetime(x)) > last_run_cron_job]
                df_update = df_consider[df_consider[table_mapping['bookmark_creation']].apply(lambda x: convert_to_datetime(x)) <= last_run_cron_job]
            else:
                df_insert, df_update = distribute_records(collection_encr, df_consider, table_mapping['table_unique_id'])
        else:
            if('bookmark_creation' in table_mapping.keys() and table_mapping['bookmark_creation']):
                df_insert = df[df[table_mapping['bookmark_creation']].apply(lambda x: convert_to_datetime(x)) > last_run_cron_job]
                df_consider = df[df[table_mapping['bookmark_creation']].apply(lambda x: convert_to_datetime(x)) <= last_run_cron_job]
                _, df_update = distribute_records(collection_encr, df_consider, table_mapping['table_unique_id'])
            else:
                df_insert, df_update = distribute_records(collection_encr, df_consider, table_mapping['table_unique_id'])
    else:
        df_insert = df_consider
        df_update = pd.DataFrame({})

    return df_insert, df_update

def get_number_of_records(db, table_name, table):
    encr_db = get_data_from_encr_db()
    table['last_run_cron_job'] = get_last_run_cron_job(encr_db, table['table_unique_id'])
    if('fetch_data_query' in table.keys() and table['fetch_data_query'] and len(table['fetch_data_query']) > 0):
        return 1
    if('username' not in db['source'].keys()):
        try:
            engine = create_engine(db['source']['url'])
            total_records = engine.execute("SELECT COUNT(*) from " + table_name).fetchall()[0][0]
            return total_records
        except:
            logging.error("sql:" + db['source']['db_name'] + ":" + table_name + ": Unable to fetch number of records.")
            return None
    else:
        try:
            conn = psycopg2.connect(
                host=db['source']['url'],
                database=db['source']['db_name'],
                user=db['source']['username'],
                password=db['source']['password'])            
            cursor = conn.cursor()
            total_records = cursor.execute("SELECT count(*) from " + table_name + ";", [])
            total_records = cursor.fetchone()[0]
            return total_records
        except:
            logging.error("sql:" + db['source']['db_name'] + ":" + table_name + ": Unable to fetch number of records.")
            return None

def get_data(db, table_name, batch_size=0, start=0, query=""):
    if('username' not in db['source'].keys()):
        try:
            engine = create_engine(db['source']['url'])
            sql_stmt = "SELECT * FROM " + table_name + " LIMIT " + str(batch_size) + " OFFSET " + str(start)
            if(len(query) > 0):
                sql_stmt = query
            df = pd.read_sql(sql_stmt, engine)
            return df
        except:
            logging.error("sql: " + db['source']['db_name'] + ":" + table_name + ": Unable to connect.")
            return None
    else:
        try:
            conn = psycopg2.connect(
                host=db['source']['url'],
                database=db['source']['db_name'],
                user=db['source']['username'],
                password=db['source']['password'])            
            select_query = "SELECT * FROM " + table_name + " LIMIT " + str(batch_size) + " OFFSET " + str(start)
            if(len(query) > 0):
                select_query = query
            df = sqlio.read_sql_query(select_query, conn)
            return df
        except:
            logging.error("sql: " + db['source']['db_name'] + ":" + table_name + ": Unable to connect.")
            return None

def process_data(df, table):
    df_insert, df_update = filter_df(df=df, table_mapping=table)
    if(df_insert is not None and df_update is not None):
        return {'name': table['table_name'], 'df_insert': df_insert, 'df_update': df_update}
    else:
        return None

def save_data(db, processed_table, partition):
    if(db['destination']['destination_type'] == 's3'):
        save_to_s3(processed_table, db_source=db['source'], db_destination=db['destination'], c_partition=partition)

def process_sql_table(db, table):
    logging.info(table['table_unique_id'] + ': Migration started.')
    if('fetch_data_query' not in table.keys() or not table['fetch_data_query']):
        table['fetch_data_query'] = ""
    
    total_len = get_number_of_records(db, table['table_name'], table)
    batch_size = 10000

    if(total_len is not None and total_len > 0):
        start = 0
        while(start < total_len):
            df = get_data(db=db, table_name=table['table_name'], batch_size=batch_size, start=start, query = table['fetch_data_query'])
            if(df is not None):
                logging.info(table['table_unique_id'] + ': Fetched data chunk.')
                try:
                    processed_table = process_data(df=df, table=table)
                    save_data(db=db, processed_table=processed_table, partition=table['partition_for_parquet'])
                except:
                    logging.error(table['table_unique_id'] + ': Caught some exception while processing/saving chunk.')
            start += batch_size
    logging.info(table['table_unique_id'] + ": Migration ended.\n")
