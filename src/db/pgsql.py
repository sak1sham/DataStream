from helper.util import convert_list_to_string, convert_to_datetime, convert_to_dtype, get_athena_dtypes
from db.encr_db import get_data_from_encr_db, get_last_run_cron_job, set_last_run_cron_job, set_last_migrated_record, get_last_migrated_record, get_last_migrated_record_prev_job, delete_metadata_from_mongodb, save_recovery_data, get_recovery_data, delete_recovery_data
from helper.exceptions import *
from helper.logger import logger
from dst.main import DMS_exporter
from helper.sigterm import GracefulKiller, NormalKiller
from notifications.slack_notify import send_message
from config.settings import settings

import pandas as pd
import psycopg2
import pymongo
import datetime
import hashlib
import pytz
from typing import List, Dict, Any, NewType, Tuple

dftype = NewType("dftype", pd.DataFrame)
collectionType =  NewType("collectionType", pymongo.collection.Collection)

class PGSQLMigrate:
    def __init__(self, db: Dict[str, Any] = None, curr_mapping: Dict[str, Any] = None, batch_size: int = 1000, tz_str: str = 'Asia/Kolkata') -> None:
        self.db = db
        self.curr_mapping = curr_mapping
        if('batch_size' in curr_mapping.keys()):
            self.batch_size = int(curr_mapping['batch_size'])
        else:
            self.batch_size = batch_size
        self.tz_info = pytz.timezone(tz_str)
        self.last_run_cron_job = pd.Timestamp(None)
        self.n_insertions = 0
        self.n_updations = 0
    

    def inform(self, message: str = None, save: bool = False) -> None:
        logger.inform(job_id=self.curr_mapping['unique_id'], s= self.curr_mapping['unique_id'] + ": " + message, save=save)


    def warn(self, message: str = None) -> None:
        logger.warn(job_id=self.curr_mapping['unique_id'], s=(self.curr_mapping['unique_id'] + ": " + message))


    def err(self, error: Any = None) -> None:
        logger.err(job_id=self.curr_mapping['unique_id'], s=error)

 
    def preprocess(self) -> None:
        '''
            Function to get the metadata from last run of job
            Also, create a saver object
        '''
        self.last_run_cron_job = get_last_run_cron_job(self.curr_mapping['unique_id'])
        self.curr_run_cron_job = pytz.utc.localize(datetime.datetime.utcnow())

        mirroring = (self.curr_mapping['mode'] == 'mirroring')
        self.saver = DMS_exporter(db = self.db, uid = self.curr_mapping['unique_id'], mirroring=mirroring, table_name=self.curr_mapping['table_name'])


    def postprocess(self):
        '''
            After completing the job, save the time when the job started (doesn't process the records that were created after the job started).
            Save the last migrated record
        '''
        self.inform(message = "Inserted " + str(self.n_insertions) + " records")
        self.inform(message = "Updated " + str(self.n_updations) + " records")
        last_rec = get_last_migrated_record(self.curr_mapping['unique_id'])
        if(last_rec):
            set_last_run_cron_job(job_id = self.curr_mapping['unique_id'], timing = self.curr_run_cron_job, last_record_id = last_rec['record_id'])
        else:
            set_last_run_cron_job(job_id = self.curr_mapping['unique_id'], timing = self.curr_run_cron_job)
        delete_recovery_data(self.curr_mapping['unique_id'])


    def distribute_records(self, collection_encr: collectionType = None, df: dftype = pd.DataFrame({}), mode: str = "both") -> Tuple[dftype]:
        '''
            Function that takes in a dataframe of records and compares the records with their hashes in order to check for insertion or updation
            mode = 'insert' or 'update' or 'both'
            'insert' does't check for records which are previously present, and returns records which are new
            'update' doesn't check for new records, only for updations in previously inserted records
            'both' checks for both updations and insertions
        '''
        df = df.sort_index(axis = 1)
        df_insert = pd.DataFrame({})
        df_update = pd.DataFrame({})
        for i in range(df.shape[0]):
            encr = {
                'table': self.curr_mapping['unique_id'],
                'map_id': df.iloc[i].unique_migration_record_id,
                'record_sha': hashlib.sha256(convert_list_to_string(df.loc[i, :].values.tolist()).encode()).hexdigest()
            }
            previous_records = collection_encr.find_one({'table': self.curr_mapping['unique_id'], 'map_id': df.iloc[i].unique_migration_record_id})
            if(previous_records):
                if(mode == 'insert'):
                    ## No need for those records which are updated
                    continue
                else:
                    if(previous_records['record_sha'] == encr['record_sha']):
                        continue
                    else:
                        df_update = df_update.append(df.loc[i, :])
                        collection_encr.delete_one({'table': self.curr_mapping['unique_id'], 'map_id': df.iloc[i].unique_migration_record_id})
                        collection_encr.insert_one(encr)
            else:
                if(mode == 'update'):
                    ## No need for those records which are inserted
                    continue
                else:
                    df_insert = df_insert.append(df.loc[i, :])
                    collection_encr.insert_one(encr)
        return df_insert, df_update


    def add_partitions(self, df: dftype = pd.DataFrame({})) -> dftype:
        '''
            Function to add partition columns to dataframe if specified in migration_mapping.
        '''
        self.partition_for_parquet = []
        if('partition_col' in self.curr_mapping.keys()):
            if(isinstance(self.curr_mapping['partition_col'], str)):
                self.curr_mapping['partition_col'] = [self.curr_mapping['partition_col']]
            if('partition_col_format' not in self.curr_mapping.keys()):
                self.curr_mapping['partition_col_format'] = ['str']
            if(isinstance(self.curr_mapping['partition_col_format'], str)):
                self.curr_mapping['partition_col_format'] = [self.curr_mapping['partition_col_format']]
            while(len(self.curr_mapping['partition_col']) > len(self.curr_mapping['partition_col_format'])):
                self.curr_mapping['partition_col_format'] = self.curr_mapping['partition_col_format'].append('str')
            for i in range(len(self.curr_mapping['partition_col'])):
                col = self.curr_mapping['partition_col'][i].lower()
                col_form = self.curr_mapping['partition_col_format'][i]
                parq_col = "parquet_format_" + col
                if(col == 'migration_snapshot_date' or col_form == 'datetime'):
                    self.partition_for_parquet.extend([parq_col + "_year", parq_col + "_month", parq_col + "_day"])
                    temp = df[col].apply(lambda x: convert_to_datetime(x, self.tz_info))
                    df[parq_col + "_year"] = temp.dt.year.astype('float64', copy=False).astype(str)
                    df[parq_col + "_month"] = temp.dt.month.astype('float64', copy=False).astype(str)
                    df[parq_col + "_day"] = temp.dt.day.astype('float64', copy=False).astype(str)
                elif(col_form == 'str'):
                    self.partition_for_parquet.extend([parq_col])
                    df[parq_col] = df[col].astype(str)
                elif(col_form == 'int'):
                    self.partition_for_parquet.extend([parq_col])
                    df[parq_col] = df[col].fillna(0).astype(int)
                else:
                    raise UnrecognizedFormat(str(col_form) + ". Partition_col_format can be int, str or datetime.") 
        else:
            self.warn(message=("Unable to find partition_col. Continuing without partitioning."))
        return df


    def dumping_data(self, df: dftype = pd.DataFrame({}), table_name: str = None, col_dtypes: Dict[str, str] = {}) -> Dict[str, Any]:
        '''
            Function that takes in dataframe and processes it assuming dumping/mirroring mode of operation
                1. Adds migration_snapshot_date column to differentiate snapshots of data
                2. Add partitions if required
                3. Final conversion of every column in dataframe to required datatypes
                4. one-on-one mapping of datatypes with athena datatypes
                5. return a processed_data object
        '''
        df['migration_snapshot_date'] = self.curr_run_cron_job
        self.partition_for_parquet = []
        if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):
            self.add_partitions(df)
        df_insert = convert_to_dtype(df, col_dtypes)
        dtypes = get_athena_dtypes(col_dtypes)
        return {'name': table_name, 'df_insert': df_insert, 'df_update': pd.DataFrame({}), 'dtypes': dtypes}


    def inserting_data(self, df: dftype = pd.DataFrame({}), table_name: str = None, col_dtypes: Dict[str, str] = {}, mode: str = None) -> Dict[str, Any]:
        '''
            Function that takes in dataframe and processes it assuming only insertion is to be performed, and no updations need to be checked
                1. Add partitions if required
                2. Create a primary_key 'unique_migration_record_id' for every record that is inserted
                3. Save records' hashes if bookmark is not present. This helps in finding updations in next run of job.
                4. Final conversion of every column in dataframe to required datatypes
                5. one-on-one mapping of datatypes with athena datatypes
                6. return a processed_data object
        '''
        if(not mode):
            raise Exception('Mode of inserting records not specified: can either be syncing or logging')
        collection_encr = get_data_from_encr_db()
        
        ## Adding partition if required
        self.partition_for_parquet = []
        if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):
            self.add_partitions(df)
 
        ## Adding a primary key "unique_migration_record_id" for every record
        df['unique_migration_record_id'] = df[self.curr_mapping['primary_key']].astype(str)
 
        ## If the bookmarks are not present in records, then save the hashes of records in order to check for updations when job is run again
        if(mode == 'syncing' and ('bookmark' not in self.curr_mapping.keys() or not self.curr_mapping['bookmark'])):
            df, _ = self.distribute_records(collection_encr, df, mode='insert')

        ## Convert to required data types and return
        df = convert_to_dtype(df, col_dtypes)
        dtypes = get_athena_dtypes(col_dtypes)
        return {'name': table_name, 'df_insert': df, 'df_update': pd.DataFrame({}), 'dtypes': dtypes}


    def updating_data(self, df: dftype = pd.DataFrame({}), table_name: str = None, col_dtypes: Dict[str, str] = {}) -> Dict[str, Any]:
        '''
            Function that takes in dataframe and processes it assuming only updations are to be performed
                1. Add partitions if required
                2. Create a primary_key 'unique_migration_record_id' for every record that is inserted
                3. If bookmark is not present, find the updations by comparing the records with their previous hashes (stored in previous run)
                4. Final conversion of every column in dataframe to required datatypes
                5. one-on-one mapping of datatypes with athena datatypes
                6. return a processed_data object
        '''
        collection_encr = get_data_from_encr_db()

        ## Adding partition if required
        self.partition_for_parquet = []
        if('to_partition' in self.curr_mapping.keys() and self.curr_mapping['to_partition']):
            self.add_partitions(df)

        ## Adding a primary key "unique_migration_record_id" for every record
        df['unique_migration_record_id'] = df[self.curr_mapping['primary_key']].astype(str)
        
        ## If the records were not already filtered as per updation_date, filter them first
        if('bookmark' not in self.curr_mapping.keys() or not self.curr_mapping['bookmark']):
            _, df = self.distribute_records(collection_encr, df, mode='update')
        
        ## Convert to required data types and return
        df = convert_to_dtype(df, col_dtypes)
        dtypes = get_athena_dtypes(col_dtypes)
        return {'name': table_name, 'df_insert': pd.DataFrame({}), 'df_update': df, 'dtypes': dtypes}


    def save_data(self, processed_data: Dict[str, Any] = None, c_partition: List[str] = None) -> None:
        '''
            processed_data is a dictionary type object with fields: name (of the table), df_insert (dataframe of records to be inserted), df_update (dataframe of records to be deleted) and dtypes (athena datatypes one to one mapping for every column in dataframe)
            This function saves the processed data into destination
        '''
        if(not processed_data):
            return
        else:
            if('name' not in processed_data.keys()):
                processed_data['name'] = self.curr_mapping['table_name']
            if('df_insert' not in processed_data.keys()):
                processed_data['df_insert'] = pd.DataFrame({})
            if('df_update' not in processed_data.keys()):
                processed_data['df_update'] = pd.DataFrame({})
            if('lob_fields_length' in self.curr_mapping.keys() and self.curr_mapping['lob_fields_length']):
                processed_data['lob_fields_length'] = self.curr_mapping['lob_fields_length']
            if('col_rename' in self.curr_mapping.keys() and self.curr_mapping['col_rename']):
                processed_data['col_rename'] = self.curr_mapping['col_rename']
            primary_keys = []
            if(self.curr_mapping['mode'] != 'dumping' and self.curr_mapping['mode'] != 'mirroring'):
                primary_keys = ['unique_migration_record_id']
            self.n_insertions += processed_data['df_insert'].shape[0]
            self.n_updations += processed_data['df_update'].shape[0]
            self.saver.save(processed_data = processed_data, primary_keys = primary_keys, c_partition = c_partition)


    def get_list_tables(self) -> List[str]:
        '''
            In case table_name is set to '*', this function helps in listing all tables in database to migrate
        '''
        sql_stmt = '''
            SELECT schemaname, tablename
            FROM pg_catalog.pg_tables
            WHERE schemaname != 'pg_catalog' AND 
            schemaname != 'information_schema';
        '''
        try:
            conn = psycopg2.connect(
                host = self.db['source']['url'],
                database = self.db['source']['db_name'],
                user = self.db['source']['username'],
                password = self.db['source']['password']
            )
            try:            
                with conn.cursor() as curs:
                    curs.execute(sql_stmt)
                    rows = curs.fetchall()
                    table_names = [str(t[0] + "." + t[1]) for t in rows]
                    return table_names
            except Exception as e:
                self.err(error=e)
                raise ProcessingError("Caught some exception while getting list of all tables.")
        except ProcessingError:
            raise
        except Exception as e:
            self.err(error=e)
            raise ConnectionError("Unable to connect to source.")


    def get_column_dtypes(self, conn: Any = None, curr_table_name: str = None) -> Dict[str, str]:
        '''
            This function executes a PGSQL query to find the datatypes of all columns in the table
        '''
        if('fetch_data_query' in self.curr_mapping.keys() and isinstance(self.curr_mapping['fetch_data_query'], str) and len(self.curr_mapping['fetch_data_query']) > 0):
            ret_dtype = {}
            if('fields' in self.curr_mapping.keys()):
                ret_dtype = self.curr_mapping['fields']
            if(self.curr_mapping['mode'] == 'dumping' or self.curr_mapping['mode'] == 'mirroring'):
                ret_dtype['migration_snapshot_date'] = 'datetime'
            return ret_dtype
        tn = curr_table_name.split('.')
        schema_name = 'public'
        table_name = ''
        if(len(tn) > 1):
            schema_name = tn[0]
            table_name = tn[1]
        else:
            # if table_name doesn't contain schema name
            table_name = tn[0]
        sql_stmt = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = \'" + table_name + "\' AND table_schema = \'" + schema_name + "\';"
        col_dtypes = {}
        with conn.cursor('cursor-to-get-col-names') as curs:
            curs.execute(sql_stmt)
            rows = curs.fetchall()
            for key, val in rows:
                col_dtypes[key] = val
        if(self.curr_mapping['mode'] == 'dumping' or self.curr_mapping['mode'] == 'mirroring'):
            col_dtypes['migration_snapshot_date'] = 'datetime'
        return col_dtypes

    
    def process_sql_query(self, table_name: str = None, sql_stmt: str = None, mode: str = "dumping", sync_mode: int = 1) -> None:
        '''
            The pgsql query is prepared by various processing functions. This function helps in processing and saving data in accordance to that PGSQL query
            sync_mode = 1 means insertion has to be performed
            sync_mode = 2 means updation has to be performed
            mode = 'syncing', 'dumping' 'mirroring' or 'logging'
        '''
        self.inform(message = str(sql_stmt))
        processed_data = {}
        processed_data_u = {}
        updated_in_destination = True
        try:
            conn = psycopg2.connect(
                host = self.db['source']['url'],
                database = self.db['source']['db_name'],
                user = self.db['source']['username'],
                password = self.db['source']['password']
            )
            try:
                col_dtypes = self.get_column_dtypes(conn = conn, curr_table_name = table_name)
                with conn.cursor('cursor-name', scrollable = True) as curs:
                    curs.itersize = 2
                    curs.execute(sql_stmt)
                    self.inform("Executed the sql statement")
                    _ = curs.fetchone()
                    columns = [desc[0] for desc in curs.description]
                    ## Now, we have the names of the columns. Next, go back right to the starting of table (-1) and fetch records from the cursor in batches.
                    curs.scroll(-1)
                    while(True):
                        rows = curs.fetchmany(self.batch_size)
                        if (not rows):
                            ## If no more rows are present, break
                            break
                        else:
                            data_df = pd.DataFrame(rows, columns = columns)
                            if(mode == "dumping" or mode == "mirroring"):
                                ## In Dumping/mirroring mode, resume mode is not supported
                                ## Processes the data in the batch and save that batch
                                ## If any error is encountered, DMS needs to restart
                                processed_data = self.dumping_data(df = data_df, table_name = table_name, col_dtypes = col_dtypes)
                                self.save_data(processed_data = processed_data, c_partition = self.partition_for_parquet)
                                processed_data = {}

                            elif(mode == "logging"):
                                ## In Logging mode, we first process and save the data of the batch
                                ## After saving every batch, we save the record_id of the last migrated record
                                ## resume mode is thus supported.
                                processed_data = self.inserting_data(df = data_df, table_name = table_name, col_dtypes = col_dtypes, mode = 'logging')
                                killer = NormalKiller()
                                if(self.curr_mapping['cron'] == 'self-managed'):
                                    killer = GracefulKiller()
                                while not killer.kill_now:
                                    self.save_data(processed_data = processed_data, c_partition = self.partition_for_parquet)
                                    if(processed_data['df_insert'].shape[0]):
                                        pkey = self.curr_mapping['primary_key']
                                        last_record_id = processed_data['df_insert'][pkey].iloc[-1]
                                        if(self.curr_mapping['primary_key_datatype'] == 'int'):
                                            last_record_id = int(last_record_id)
                                        elif(self.curr_mapping['primary_key_datatype'] == 'str'):
                                            last_record_id = str(last_record_id)
                                        elif(self.curr_mapping['primary_key_datatype'] == 'datetime'):
                                            if(not last_record_id.tzinfo):
                                                last_record_id = self.tz_info.localize(last_record_id)
                                            last_record_id = last_record_id.astimezone(pytz.utc)
                                        set_last_migrated_record(job_id = self.curr_mapping['unique_id'], _id = last_record_id, timing = datetime.datetime.utcnow())
                                    processed_data = {}
                                    break
                                if(killer.kill_now):
                                        msg = "Migration stopped for *{0}* from database *{1}* ({2}) to *{3}*\n".format(self.curr_mapping['table_name'], self.db['source']['db_name'], self.db['source']['source_type'], self.db['destination']['destination_type'])
                                        msg += "Reason: Caught sigterm :warning:\n"
                                        msg += "Insertions: {0}\nUpdations: {1}".format("{:,}".format(self.n_insertions), "{:,}".format(self.n_updations))
                                        slack_token = settings['slack_notif']['slack_token']
                                        channel = self.curr_mapping['slack_channel'] if 'slack_channel' in self.curr_mapping and self.curr_mapping['slack_channel'] else settings['slack_notif']['channel']
                                        send_message(msg = msg, channel = channel, slack_token = slack_token)
                                        self.inform('Notification sent.')
                                        raise KeyboardInterrupt("Ending gracefully.")

                            elif(mode == "syncing"):
                                if(sync_mode == 1):
                                    ## INSERTION MODE
                                    ## In syncing-insertion mode, we first process and save the data of the batch
                                    ## After saving every batch, we save the record_id of the last migrated record
                                    ## resume mode is thus supported.
                                    processed_data = self.inserting_data(df = data_df, table_name = table_name, col_dtypes = col_dtypes, mode = 'syncing')
                                    killer = NormalKiller()
                                    if(self.curr_mapping['cron'] == 'self-managed'):
                                        killer = GracefulKiller()
                                    while not killer.kill_now:
                                        self.save_data(processed_data = processed_data, c_partition = self.partition_for_parquet)
                                        if(processed_data['df_insert'].shape[0]):
                                            pkey = self.curr_mapping['primary_key']
                                            last_record_id = processed_data['df_insert'][pkey].iloc[-1]
                                            if(self.curr_mapping['primary_key_datatype'] == 'int'):
                                                last_record_id = int(last_record_id)
                                            elif(self.curr_mapping['primary_key_datatype'] == 'str'):
                                                last_record_id = str(last_record_id)
                                            elif(self.curr_mapping['primary_key_datatype'] == 'datetime'):
                                                if(not last_record_id.tzinfo):
                                                    last_record_id = self.tz_info.localize(last_record_id)
                                                last_record_id = last_record_id.astimezone(pytz.utc)
                                            set_last_migrated_record(job_id = self.curr_mapping['unique_id'], _id = last_record_id, timing = datetime.datetime.utcnow())
                                            save_recovery_data(job_id = self.curr_mapping['unique_id'], _id = last_record_id, timing = self.curr_run_cron_job)
                                        processed_data = {}
                                        break
                                    if(killer.kill_now):
                                        msg = "Migration stopped for *{0}* from database *{1}* ({2}) to *{3}*\n".format(self.curr_mapping['table_name'], self.db['source']['db_name'], self.db['source']['source_type'], self.db['destination']['destination_type'])
                                        msg += "Reason: Caught sigterm :warning:\n"
                                        msg += "Insertions: {0}\nUpdations: {1}".format("{:,}".format(self.n_insertions), "{:,}".format(self.n_updations))
                                        slack_token = settings['slack_notif']['slack_token']
                                        channel = self.curr_mapping['slack_channel'] if 'slack_channel' in self.curr_mapping and self.curr_mapping['slack_channel'] else settings['slack_notif']['channel']
                                        send_message(msg = msg, channel = channel, slack_token = slack_token)
                                        self.inform('Notification sent.')
                                        raise KeyboardInterrupt("Ending gracefully.")
                                else:
                                    ## UPDATION MODE
                                    ## In syncing-update mode, we iterate through multiple batches until we find atleast (batch_size) number of records to be updates
                                    ## Then, after considerable number of records are found that needs to be updated, update them collectively to save time.
                                    ## Resume operation is automatically supported (last_run_cron_job is changes after job is successful, if not, then the updation will start again for records updated after previous job)
                                    processed_data_u = {}
                                    processed_data_u = self.updating_data(df = data_df, table_name = table_name, col_dtypes = col_dtypes)
                                    if(processed_data_u):
                                        if(not updated_in_destination):
                                            processed_data['df_update'] = processed_data['df_update'].append([processed_data_u['df_update']])
                                        else:
                                            processed_data['df_update'] = processed_data_u['df_update']
                                        self.inform(message="Found " + str(processed_data['df_update'].shape[0]) + " updations upto now.")
                                    if(processed_data['df_update'].shape[0] >= self.batch_size):
                                        self.save_data(processed_data = processed_data, c_partition = self.partition_for_parquet)
                                        processed_data = {}
                                        updated_in_destination = True
                                    else:
                                        updated_in_destination = False                            
                self.inform(message="Completed processing of table " + table_name + ".")
            except Exception as e:
                self.err(error=e)
                raise ProcessingError("Caught some exception while processing records.")
        except ProcessingError:
            raise
        except Exception as e:
            self.err(error=e)
            raise ConnectionError("Unable to connect to source.")

        if(mode == 'syncing' and sync_mode == 2 and processed_data):
            ## If processing updates are completed, and total updated records present in processed_data are less than batch_size, still save them now
            self.save_data(processed_data = processed_data, c_partition = self.partition_for_parquet)
            processed_data = {}


    def get_last_pkey(self, table_name: str = None) -> Any:
        '''
            Function to return the maximum value of from primary_key column of the table.
            primary_key can be either string, integer or datetime.
            primary_key needs to be unique, and strictly increasing.
        '''
        sql_stmt = 'SELECT max(' + self.curr_mapping['primary_key'] + ') as curr_max_pkey FROM ' + table_name
        try:
            conn = psycopg2.connect(
                host = self.db['source']['url'],
                database = self.db['source']['db_name'],
                user = self.db['source']['username'],
                password = self.db['source']['password']
            )
            try:
                with conn.cursor('cursor-name', scrollable = True) as curs:
                    curs.itersize = 2
                    curs.execute(sql_stmt)
                    curr_max_pkey = curs.fetchone()[0]
                    return curr_max_pkey
            except Exception as e:
                self.err(error=e)
                raise ProcessingError("Caught some exception while finding maximum value of primary_key till now.")
        except ProcessingError:
            raise
        except Exception as e:
            self.err(error=e)
            raise ConnectionError("Unable to connect to source.")


    def dumping_process(self, table_name: str = None) -> None:
        '''
            Function to create PGSQL query for dumping/mirroring mode and then send it further for processing.
        '''
        sql_stmt = "SELECT * FROM " + table_name
        if('fetch_data_query' in self.curr_mapping.keys() and self.curr_mapping['fetch_data_query'] and len(self.curr_mapping['fetch_data_query']) > 0):
            sql_stmt = self.curr_mapping['fetch_data_query']
        self.process_sql_query(table_name, sql_stmt, mode=self.curr_mapping['mode'])


    def logging_process(self, table_name: str = None) -> None:
        '''
            Function to create PGSQL query for logging mode and then send it further for processing.
        '''
        sql_stmt = "SELECT * FROM " + table_name
        if('fetch_data_query' in self.curr_mapping.keys() and self.curr_mapping['fetch_data_query'] and len(self.curr_mapping['fetch_data_query']) > 0):
            raise IncorrectMapping("Can not have custom query (fetch_data_query) in logging or syncing mode.")
       
        if(self.curr_mapping['primary_key_datatype'] == 'int'):
            last_rec = get_last_migrated_record(self.curr_mapping['unique_id'])
            last = -2147483648
            curr = int(self.get_last_pkey(table_name = table_name))
            if(last_rec):
                last = int(last_rec['record_id'])
            curr = str(curr)
            last = str(last)
            sql_stmt += " WHERE " + self.curr_mapping['primary_key'] + " > " + last + " AND " + self.curr_mapping['primary_key'] + " <= " + curr
        elif(self.curr_mapping['primary_key_datatype'] == 'str'):
            last_rec = get_last_migrated_record(self.curr_mapping['unique_id'])
            last = ""
            curr = str(self.get_last_pkey(table_name = table_name))
            if(last_rec):
                last = str(last_rec['record_id'])
            sql_stmt += " WHERE " + self.curr_mapping['primary_key'] + " > \'" + last + "\' AND " + self.curr_mapping['primary_key'] + " <= \'" + curr + "\'"
        elif(self.curr_mapping['primary_key_datatype'] == 'datetime'):
            last_rec = get_last_migrated_record(self.curr_mapping['unique_id'])
            last = datetime.datetime(2000, 1, 1, 0, 0, 0, 0, self.tz_info)
            curr = self.get_last_pkey(table_name = table_name)
            if(curr.tzinfo):
                curr = curr.astimezone(pytz.utc)
            else:
                curr = self.tz_info.localize(curr)
            if(last_rec):
                last = pytz.utc.localize(last_rec['record_id']).astimezone(self.tz_info)
            sql_stmt += " WHERE Cast(" + self.curr_mapping['primary_key'] + " as timestamp) > Cast(\'" + last.strftime('%Y-%m-%d %H:%M:%S') + "\' as timestamp) AND Cast(" + self.curr_mapping['primary_key'] + " as timestamp) <= Cast(\'" + curr.strftime('%Y-%m-%d %H:%M:%S') + "\' as timestamp)"
        else:
            IncorrectMapping("primary_key_datatype can either be str, or int or datetime.")
        sql_stmt += " ORDER BY " + self.curr_mapping['primary_key'] 
        self.process_sql_query(table_name, sql_stmt, mode='logging')


    def syncing_process(self, table_name: str = None) -> None:
        '''
            Function to create PGSQL query for logging mode and then send it further for processing.
            2-step process: Insertion followed by Deletion
        '''
        ## FIRST WE NEED TO UPDATE THOSE RECORDS WHICH WERE INSERTED IN LAST RUN, BUT AN ERROR WAS ENCOUNTERED, AND THOSE WERE UPDATED AT SOURCE LATER ON
        recovery_data = get_recovery_data(self.curr_mapping['unique_id'])
        if(recovery_data):
            self.inform("")
            self.inform("Trying to make the system recover by updating the records inserted during the previously failed job(s).")
            last2 = recovery_data['record_id']
            sql_stmt = "SELECT * FROM " + table_name
            if(self.curr_mapping['primary_key_datatype'] == 'int'):
                last_rec = get_last_migrated_record_prev_job(self.curr_mapping['unique_id'])
                last = -2147483648
                if(last_rec):
                    last = int(last_rec)
                last = str(last)
                sql_stmt += " WHERE {0} > {1} AND {2} <= {3}".format(self.curr_mapping['primary_key'], last, self.curr_mapping['primary_key'], last2)
            elif(self.curr_mapping['primary_key_datatype'] == 'str'):
                last_rec = get_last_migrated_record_prev_job(self.curr_mapping['unique_id'])
                last = ""
                if(last_rec):
                    last = str(last_rec)
                sql_stmt += " WHERE {0} > \'{1}\' AND {2} <= \'{3}\'".format(self.curr_mapping['primary_key'], last, self.curr_mapping['primary_key'], last2)
            elif(self.curr_mapping['primary_key_datatype'] == 'datetime'):
                last_rec = get_last_migrated_record_prev_job(self.curr_mapping['unique_id'])
                last = datetime.datetime(2000, 1, 1, 0, 0, 0, 0, self.tz_info)
                if(last_rec):
                    last = pytz.utc.localize(last_rec).astimezone(self.tz_info)
                sql_stmt += " WHERE CAST({0} as timestamp) > CAST(\'{1}\' as timestamp) AND CAST({2} as timestamp) <= CAST(\'{3}\' as timestamp)".format(self.curr_mapping['primary_key'], last.strftime('%Y-%m-%d %H:%M:%S'), self.curr_mapping['primary_key'], last2.strftime('%Y-%m-%d %H:%M:%S'))
            else:
                IncorrectMapping("primary_key_datatype can either be str, or int or datetime.")
            
            last_time = pytz.utc.localize(recovery_data['timing'])
            last_time = last_time.astimezone(self.tz_info).strftime('%Y-%m-%d %H:%M:%S')
            if('bookmark' in self.curr_mapping.keys() and self.curr_mapping['bookmark']): 
                if('improper_bookmarks' in self.curr_mapping.keys() and self.curr_mapping['improper_bookmarks']): 
                    sql_stmt += " AND Cast({0} as timestamp) > CAST(\'{1}\' as timestamp)".format(self.curr_mapping['bookmark'], last_time)
                else: 
                    sql_stmt += " AND {0} > \'{1}\'::timestamp".format(self.curr_mapping['bookmark'], last_time)
            self.process_sql_query(table_name, sql_stmt, mode='syncing', sync_mode = 2)

        ## NOW, SYSTEM IS RECOVERED, LET'S FOCUS ON INSERTING NEW DATA
        self.inform("")
        self.inform("Starting Insertion of new records.")
        sql_stmt = "SELECT * FROM " + table_name
        if('fetch_data_query' in self.curr_mapping.keys() and self.curr_mapping['fetch_data_query'] and len(self.curr_mapping['fetch_data_query']) > 0):
            raise IncorrectMapping("Can not have custom query (fetch_data_query) in logging or syncing mode.")
        
        if(self.curr_mapping['primary_key_datatype'] == 'int'):
            last_rec = get_last_migrated_record(self.curr_mapping['unique_id'])
            last = -2147483648
            curr_max = int(self.get_last_pkey(table_name = table_name))
            if(last_rec):
                last = int(last_rec['record_id'])
            curr_max = str(curr_max)
            last = str(last)
            sql_stmt += " WHERE " + self.curr_mapping['primary_key'] + " > " + last + " AND " + self.curr_mapping['primary_key'] + " <= " + curr_max
        elif(self.curr_mapping['primary_key_datatype'] == 'str'):
            last_rec = get_last_migrated_record(self.curr_mapping['unique_id'])
            last = ""
            curr_max = str(self.get_last_pkey(table_name = table_name))
            if(last_rec):
                last = str(last_rec['record_id'])
            sql_stmt += " WHERE " + self.curr_mapping['primary_key'] + " > \'" + last + "\' AND " + self.curr_mapping['primary_key'] + " <= \'" + curr_max + "\'"
        elif(self.curr_mapping['primary_key_datatype'] == 'datetime'):
            last_rec = get_last_migrated_record(self.curr_mapping['unique_id'])
            last = datetime.datetime(2000, 1, 1, 0, 0, 0, 0, self.tz_info)
            curr_max = self.get_last_pkey(table_name = table_name)
            if(curr_max.tzinfo):
                curr_max = curr_max.astimezone(pytz.utc)
            else:
                curr_max = self.tz_info.localize(curr_max)
            if(last_rec):
                last = pytz.utc.localize(last_rec['record_id']).astimezone(self.tz_info)
            sql_stmt += " WHERE Cast(" + self.curr_mapping['primary_key'] + " as timestamp) > Cast(\'" + last.strftime('%Y-%m-%d %H:%M:%S') + "\' as timestamp) AND Cast(" + self.curr_mapping['primary_key'] + " as timestamp) <= Cast(\'" + curr_max.strftime('%Y-%m-%d %H:%M:%S') + "\' as timestamp)"
        else:
            IncorrectMapping("primary_key_datatype can either be str, or int or datetime.")
        sql_stmt += " ORDER BY " + self.curr_mapping['primary_key'] 
        self.process_sql_query(table_name, sql_stmt, mode='syncing', sync_mode = 1)
        self.inform("Inserted all records found.")
        
        ## NOW INSERTION IS COMPLETE, LET'S FOCUS ON UPDATING OLD DATA
        self.inform("")
        self.inform("Starting updation of previously existing records.")
        sql_stmt = "SELECT * FROM " + table_name
        if(self.curr_mapping['primary_key_datatype'] == 'int'):
            last_rec = get_last_migrated_record_prev_job(self.curr_mapping['unique_id'])
            last = -2147483648
            if(last_rec):
                last = int(last_rec)
            last = str(last)
            sql_stmt += " WHERE " + self.curr_mapping['primary_key'] + " <= " + last
        elif(self.curr_mapping['primary_key_datatype'] == 'str'):
            last_rec = get_last_migrated_record_prev_job(self.curr_mapping['unique_id'])
            last = ""
            if(last_rec):
                last = str(last_rec)
            sql_stmt += " WHERE " + self.curr_mapping['primary_key'] + " <= \'" + last + "\'"
        elif(self.curr_mapping['primary_key_datatype'] == 'datetime'):
            last_rec = get_last_migrated_record_prev_job(self.curr_mapping['unique_id'])
            last = datetime.datetime(2000, 1, 1, 0, 0, 0, 0, self.tz_info)
            if(last_rec):
                last = pytz.utc.localize(last_rec).astimezone(self.tz_info)
            sql_stmt += " WHERE Cast(" + self.curr_mapping['primary_key'] + " as timestamp) <= Cast(\'" + last.strftime('%Y-%m-%d %H:%M:%S') + "\' as timestamp)"
        else:
            IncorrectMapping("primary_key_datatype can either be str, or int or datetime.")
        
        last = self.last_run_cron_job
        if('grace_updation_lag' in self.curr_mapping.keys() and self.curr_mapping['grace_updation_lag']):
            '''
                parameter to pass to double-check for any updations missed.
            '''
            days = 0
            hours = 0
            minutes = 0
            if('days' in self.curr_mapping['grace_updation_lag'].keys()):
                days = self.curr_mapping['grace_updation_lag']['days']
            if('hours' in self.curr_mapping['grace_updation_lag'].keys()):
                hours = self.curr_mapping['grace_updation_lag']['hours']
            if('minutes' in self.curr_mapping['grace_updation_lag'].keys()):
                minutes = self.curr_mapping['grace_updation_lag']['minutes']
            last = last - datetime.timedelta(days=days, hours=hours, minutes=minutes)

        last = last.astimezone(self.tz_info).strftime('%Y-%m-%d %H:%M:%S')
        if('bookmark' in self.curr_mapping.keys() and self.curr_mapping['bookmark']): 
            if('improper_bookmarks' in self.curr_mapping.keys() and self.curr_mapping['improper_bookmarks']): 
                sql_stmt += " AND Cast(" + self.curr_mapping['bookmark'] + " as timestamp) > CAST(\'" + last + "\' as timestamp)" 
            else: 
                sql_stmt += " AND " + self.curr_mapping['bookmark'] + " > \'" + last + "\'::timestamp" 
        self.process_sql_query(table_name, sql_stmt, mode='syncing', sync_mode = 2)
        self.inform("Updated all existing records.")

        ## NOW, UPDATION IS ALSO COMPLETE
        ## WE NEED TO UPDATE/DOUBLE-CHECK THAT DATA WHICH WAS INSERTED DURING CURRENT MIGRATION, BUT UPDATED 2-3 MINUTES BEFORE THE JOB STARTED
        if('buffer_updation_lag' in self.curr_mapping.keys() and self.curr_mapping['buffer_updation_lag']):
            buffer_hours = 0 if 'hours' not in self.curr_mapping['buffer_updation_lag'].keys() or not self.curr_mapping['buffer_updation_lag']['hours'] else self.curr_mapping['buffer_updation_lag']['hours']
            buffer_minutes = 0 if 'minutes' not in self.curr_mapping['buffer_updation_lag'].keys() or not self.curr_mapping['buffer_updation_lag']['minutes'] else self.curr_mapping['buffer_updation_lag']['minutes']
            buffer_seconds = 0 if 'seconds' not in self.curr_mapping['buffer_updation_lag'].keys() or not self.curr_mapping['buffer_updation_lag']['seconds'] else self.curr_mapping['buffer_updation_lag']['seconds']
            self.inform("")
            self.inform("Starting updation-check for newly inserted records which were updated in the {0} hours, {1} minutes and {2} minutes before the job started.".format(buffer_hours, buffer_minutes, buffer_seconds))
            curr = get_last_migrated_record(self.curr_mapping['unique_id'])
            if(curr and self.n_insertions > 0 and 'record_id' in curr.keys() and curr['record_id']):
                ## If some records were inserted, we need to check updates for last few records as per precise time 
                curr = curr['record_id']
                sql_stmt = "SELECT * FROM " + table_name
                if(self.curr_mapping['primary_key_datatype'] == 'int'):
                    sql_stmt += " WHERE {0} <= {1}".format(self.curr_mapping['primary_key'], str(int(float(curr))))
                elif(self.curr_mapping['primary_key_datatype'] == 'str'):
                    sql_stmt += " WHERE {0} <= \'{1}\'".format(self.curr_mapping['primary_key'], curr)
                elif(self.curr_mapping['primary_key_datatype'] == 'datetime'):
                    curr = pytz.utc.localize(curr).astimezone(self.tz_info)
                    sql_stmt += " WHERE {0} <= CAST(\'{1}\' as timestamp)".format(self.curr_mapping['primary_key'], curr)
                else:
                    IncorrectMapping("primary_key_datatype can either be str, or int or datetime.")
                
                last1 = (self.curr_run_cron_job - datetime.timedelta(hours=buffer_hours, minutes=buffer_minutes, seconds=buffer_seconds)).astimezone(self.tz_info).strftime('%Y-%m-%d %H:%M:%S')
                last2 = self.curr_run_cron_job.astimezone(self.tz_info).strftime('%Y-%m-%d %H:%M:%S')
                if('bookmark' in self.curr_mapping.keys() and self.curr_mapping['bookmark']): 
                    if('improper_bookmarks' in self.curr_mapping.keys() and self.curr_mapping['improper_bookmarks']): 
                        sql_stmt += " AND Cast({0} as timestamp) > CAST(\'{1}\' as timestamp) AND Cast({2} as timestamp) <= CAST(\'{3}\' as timestamp)".format(self.curr_mapping['bookmark'], last1, self.curr_mapping['bookmark'], last2)
                    else:
                        sql_stmt += " AND {0} > \'{1}\'::timestamp AND {2} <= \'{3}\'::timestamp".format(self.curr_mapping['bookmark'], last1, self.curr_mapping['bookmark'], last2) 
                self.process_sql_query(table_name, sql_stmt, mode='syncing', sync_mode = 2)
                self.inform("Double-checked for updations in last {0} hours, {1} minutes and {2} minutes.".format(buffer_hours, buffer_minutes, buffer_seconds))


    def preprocess_table(self, table_name: str = None) -> None:
        n_columns_redshift = self.saver.get_n_redshift_cols(table_name=table_name)
        try:
            conn = psycopg2.connect(
                host = self.db['source']['url'],
                database = self.db['source']['db_name'],
                user = self.db['source']['username'],
                password = self.db['source']['password']
            )
            col_dtypes = self.get_column_dtypes(conn = conn, curr_table_name = table_name)
            n_columns_pgsql = len(col_dtypes) + 1
            ## 1 is added because in logging and syncing operations, unique_migration_record_id is present
            ## In case of dumping/mirroring, migration_snapshot_date is present
            ## we also need to account for those columns which are partitioned
            if('partition_col' in self.curr_mapping.keys()):
                if(isinstance(self.curr_mapping['partition_col'], str)):
                    self.curr_mapping['partition_col'] = [self.curr_mapping['partition_col']]
                if('partition_col_format' not in self.curr_mapping.keys()):
                    self.curr_mapping['partition_col_format'] = ['str']
                if(isinstance(self.curr_mapping['partition_col_format'], str)):
                    self.curr_mapping['partition_col_format'] = [self.curr_mapping['partition_col_format']]
                while(len(self.curr_mapping['partition_col']) > len(self.curr_mapping['partition_col_format'])):
                    self.curr_mapping['partition_col_format'] = self.curr_mapping['partition_col_format'].append('str')
                for i in range(len(self.curr_mapping['partition_col'])):
                    col = self.curr_mapping['partition_col'][i].lower()
                    col_form = self.curr_mapping['partition_col_format'][i]
                    if(col == 'migration_snapshot_date' or col_form == 'datetime'):
                        n_columns_pgsql += 3
                    elif(col_form == 'str'):
                        n_columns_pgsql += 1
                    elif(col_form == 'int'):
                        n_columns_pgsql += 1
            if(n_columns_redshift > 0 and n_columns_pgsql != n_columns_redshift):
                self.warn("There is a mismatch in columns present in source and destination. Deleting data from destination and encr-db and then re-migrating.")
                self.saver.drop_redshift_table(table_name=table_name)
                delete_metadata_from_mongodb(self.curr_mapping['unique_id'])
                self.inform("Data is deleted, now starting migration again.")
            else:
                self.inform('No discrepancy, let\'s start the migration')
        except ProcessingError:
            raise Exception("Unable to verify datatypes of table from source and destination.")


    def process(self) -> Tuple[int]:
        if(self.curr_mapping['mode'] != 'dumping' and self.curr_mapping['mode'] != 'mirroring' and ('primary_key' not in self.curr_mapping.keys() or not self.curr_mapping['primary_key'])):
            raise IncorrectMapping('Need to specify a primary_key (strictly increasing and unique - int|string|datetime) inside the table for syncing or logging mode.')
        elif(self.curr_mapping['mode'] != 'dumping' and self.curr_mapping['mode'] != 'mirroring' and 'primary_key_datatype' not in self.curr_mapping.keys()):
            raise IncorrectMapping('primary_key_datatype not specified. Please specify primary_key_datatype as either str or int or datetime.')

        if(self.curr_mapping['mode'] == 'mirroring' and self.db['destination']['destination_type'] == 's3'):
            raise IncorrectMapping("Mirroring mode not supported for destination S3")
        if(self.curr_mapping['mode'] == 'mirroring' and self.curr_mapping['table_name'] == '*'):
            raise IncorrectMapping("Can not migrate all tables together in mirroring mode. Please specify a table_name.")
        
        if('username' not in self.db['source'].keys()):
            self.db['source']['username'] = ''
        if('password' not in self.db['source'].keys()):
            self.db['source']['password'] = ''
        
        name_tables = []
        if(self.curr_mapping['table_name'] == '*'):
            name_tables = self.get_list_tables()
        else:
            name_tables = [self.curr_mapping['table_name']]
        name_tables.sort()
        self.inform(message="Found following " + str(len(name_tables)) + " tables from database " + str(self.db['source']['db_name']) + ":\n" + '\n'.join(name_tables))
        
        b_start = 0
        b_end = len(name_tables)
        if('batch_start' in self.curr_mapping.keys()):
            b_start = self.curr_mapping['batch_start']
        if('batch_end' in self.curr_mapping.keys()):
            b_end = self.curr_mapping['batch_end']
        name_tables = name_tables[b_start:b_end]

        self.preprocess()
        self.inform(message="Mapping pre-processed.", save=True)
        
        if('exclude_tables' not in self.curr_mapping.keys()):
            self.curr_mapping['exclude_tables'] = []
        elif(isinstance(self.curr_mapping['exclude_tables'], str)):
            self.curr_mapping['exclude_tables'] = [self.curr_mapping['exclude_tables']]
        useful_tables = []
        for name_ in name_tables:
            if(name_ not in self.curr_mapping['exclude_tables']):
                useful_tables.append(name_)
        name_tables = useful_tables
        name_tables.sort()
        self.inform(message="Starting to migrating following " + str(len(name_tables)) + " useful tables from database " + str(self.db['source']['db_name']) + ":\n" + '\n'.join(name_tables), save=True)
        
        for table_name in name_tables:
            if(table_name.count('.') >= 2):
                self.warn(message=("Can not migrate table with table_name: " + table_name))
                continue
            if(self.db['destination']['destination_type'] == 'redshift'):
                self.preprocess_table(table_name)
            
            if(self.curr_mapping['mode'] == 'dumping'):
                self.dumping_process(table_name)
            elif(self.curr_mapping['mode'] == 'logging'):
                self.logging_process(table_name)
            elif(self.curr_mapping['mode'] == 'syncing'):
                self.syncing_process(table_name)
            elif(self.curr_mapping['mode'] == 'mirroring'):
                self.dumping_process(table_name)
            else:
                raise IncorrectMapping("Wrong mode of operation: can be syncing, logging, mirroring or dumping only.")
            self.inform(message=("Migration completed for table " + str(table_name)), save=True)
                
        self.inform(message="Overall migration complete.", save=True)
        if(self.curr_mapping['mode'] == 'dumping' and 'expiry' in self.curr_mapping.keys() and self.curr_mapping['expiry']):
            self.saver.expire(expiry = self.curr_mapping['expiry'], tz_info = self.tz_info)
            self.inform(message="Expired data removed.", save=True)

        self.postprocess()
        self.inform(message="Post processing completed.", save=True)

        self.saver.close()
        self.inform(message="Hope to see you again :')")

        return (self.n_insertions, self.n_updations)
