# Settings

```fastapi_server```: (bool) Whether the fastapi server needs to be kept running in background. Jobs can be scheduled only if this field is set to True. Default=False

```timezone```: (str) the default timezone that DMS script considers. As per pytz specifications. Default='Asia/Kolkata'.

```notify```: (bool) Whether slack notifications need to be setup. Notified after completion of any migration, or when migration stops due to some exception. Default=False

```encryption_store```: (Dict[str, str]) Required. Connection to a MongoDB type database. All information about running jobs (example - last run time, last primary key inserted, etc.) is stored here. ```url```, ```db_name```, ```collection_name``` needs to be set as keys within this dictionary.

```dashboard_store```: (Dict[str, str]) Required. Connection to a MongoDB type database. All information about (un)finished jobs (example - total records migrated, storage size, time taken, etc.) is stored here. ```url```, ```db_name```, ```collection_name``` needs to be set as keys within this dictionary. The information stored in this location can help in creating a visibility dashboard for all running jobs.

```slack_notif```: (Dict[str, str]). Credentials for slack notifications. Need to be provided in case slack_notify is set to True. All information about finished jobs are notified here. The unfinished jobs are also notified with a properly formatted exception message. ```slack_token```, ```channel``` needs to be set as keys within this dictionary. Default={}

```cut_off_time```: (datetime.time, without timezone) If provided, will shut down the running jobs once the current time reaches cutoff time. Default=None

Check out [this sample file](../../sample_settings/settings.py) to learn how to create your own settings.

# Job Mapping

## Intro

```primary_key```: The data field which is uniquely represents some record/document within a table/collection/api. For example: id, roll_number, etc

```bookmark```: A datetime type data field present in every record/document, which gets automatically changed to NOW() whenever that record/document is modified. For example: updated_at, modified_at, etc.

## Mapping file format
pipeline_format is a dictionary with following keys:
1. source
2. destination
3. tables (for source pgsql), or collections (for source mongodb), or apis (for source api), or topics (for source kafka)

For example:
```python
mapping = {
    'source': {
        'source_type': 'pgsql'
        ...
    },
    'destination': {...},
    'tables': [...]
}
```

### Source
```python
'source': {
    'source_type': 'mongo', 
    ## str: Required, can be pgsql, mongo, api, or kafka,
    
    'url': 'my.connection.url',  
    ## str: Required, url for the data source
    
    'db_name': 'my-db-name',  
    ## str: Required, name of source dabatase. In case of kafka, a dummy db_name can be provided
    
    'username': '', 
    ## str: Optional, username to connect to the db
    
    'password': ''  
    ## str: Optional, password to connect to the db
},
```

### Destination
```python
'destination': {        
    ## Zero or more destinations can be provided, as key-value pairs of unique-names and respective destination specifications
    
    ## First destination. 'dest-1' is a user-defined and unique name for the destination
    "dest-1": {         
        'destination_type': 's3',   
        ## str: Required, type of destination (from s3, redshift, pgsql)
        
        's3_bucket_name': 'my-s3-bucket-name',  
        ## str: Required when destination_type is 's3' otherwise optional, name of s3-bucket which will be used to store data at destination
        
        's3_suffix': 'my_suffix'    
        ## str: Optional, the suffix to be added to name of table (Athena, and S3 folder) at destination
    },
    
    ## Second destination. 'my_dest_2' is a user-defined and unique name for the destination
    "my_dest_2": {      
        'destination_type': 'redshift',   
        ## str: Required, type of destination (from s3, redshift, pgsql)
        
        'host': 'examplecluster.abc123xyz789.us-west-1.redshift.amazonaws.com',  
        ## str: Required, host url to connect to redshift
        
        'database': 'redshift-db-name',     
        ## str: Required, database to connect in redshift
        
        'user': os.getenv('REDSHIFT_USER'), 
        ## str: Required, username credentials for redshift connection
        
        'password': os.getenv('REDSHIFT_PASSWORD'), 
        ## str: Required, password credentials for redshift connection
        
        's3_bucket_name': 's3-bucket-name', 
        ## str: Required, for faster migration, the data is first saved to s3 bucket and then transferred to redshift. 
        ## No data will be stored in this bucket, but is required as a temporary middleware
    }
    
    ## Third destination. 'pgsql_destination_3' is a user-defined and unique name for the destination
    'pgsql_destination_3': {    
        "destination_type": "pgsql",    
        ## str: Required, type of destination (from s3, redshift, pgsql)
        
        "url": "destination.connection.url",    
        ## str: Required, host url to connect to destination
        
        "db_name": "database-name",     
        ## str: Required, database to connect in destination pgsql
        
        "username": os.getenv('DB_USERNAME'),   
        ## str: Required, credentials for destination connection
        
        "password": os.getenv('DB_PASSWORD'),   
        ## str: Required, credentials for destination connection
        
        "schema": "public"  
        ## str: Optional, schema to migrate to at destination. 
        ## If not provided, data is migrated to a new schema: {source_name}_{source_db_name}_dms. 
        ## For example - if schema is not provided in the above source, the data will be migrated to schema "mongo_my-db-name_dms". 
        ## In case of pgsql at destination, "sql" is used instead of "pgsql".
    },
},
```

### Data structuring
#### Tables
If source type is PgSQL, then the "tables" specification needs to be provided to identify the table characteristics at source

"tables": List[Dict[str, Any]]
```python
mapping = {
    'source': {
        'source_type': 'pgsql'
        ...
    },
    'destination': {...},
    'tables': [
        ## tables is a list. Each element of this list corresponds to a table specification (dictionary)
        ## Here just a single table is specified for the sake of simplicity.
        {
            ## Table 1
            "table_name": "my_table",
            ## str: Required, Name of the table at source

            "cron": "self-managed",
            ## str: Required: "self-managed" or a specific cron-schedule like '* * * * * 7-19 */1 0' (Refer Notes 1)
            ## If cron is self-managed, the script will start immediately, without any internal scheduler.
            ## If a specific cron is provided (As per Notes 1), the script will use APScheduler to interally schedule the migration

            "mode": "mirroring",
            ## str: Reqruired, Mode of migration: dumping, logging syncing, or mirroring
            ## Refer Notes 2 for understanding the 4 modes of operations.
            
            "batch_size": 10000,
            ## int: Optional, the size of the batch/chunks. Default=10000
            
            "bookmark": "updated_at",
            ## str: Required if mode is syncing or mirroring, otherwise Optional
            ## Refer Notes 3 for understanding what are bookmarks
            
            "improper_bookmarks": False,
            ## bool: Optional (Default = False)
            ## set this as true in case the bookmark fields at source have datetimes in any other standard format (like string)
            
            "primary_key": "id",
            ## str: Required for all except dumping mode
            ## To keep track of unique records for each table
            ## For logging mode, the primary key shall be auto-incremental
            
            "primary_key_datatype": "uuid",
            ## str: Required for all except dumping mode
            ## This specified the datatype of the primary_key, can be str, uuid, int, or datetime
            
            'partition_col': 'created_at',
            ## str or False: Optional (Default=False). Useful when destination is pgsql or s3.
            ## If False, the data won't be partitioned at destination
            ## Otherwise, data partitioning will be performed. 
            ## For destionation is pgsql, only datetime partitions are useful, and the monthly partitions will be created from 2015-2030

            'partition_col_format': 'datetime',
            ## str: Optional (Default = str): can be datetime, int, or str
            ## The datatype of the partition column at source
            
            "buffer_updation_lag": {
                "hours": 2
            },
            ## Dict[str, int]: Optional. In case the data is frequently updated, this helps in double-checking the records updated between (migration_start_time - buffer_updation_lag) to (migration_start_time), where migration_start_time is the time of start of migration 
            ## hours, days and minutes can be specified

            "grace_updation_lag": {
                "days": 1
            },
            ## Dict[str, int]: Optional. In case the data is frequently updated, this helps in double-checking the records updated in the grace_updation_lag before the previous successful migration
            ## For example, here days = 1, and if we are running this script on a daily basis, and the job was successful yesterday, then today it will again check for all updates which happened day-before-yeterday to yesterday
            ## hours, days and minutes can be specified
            
            'strict': True,
            ## bool: Optional. Useful when destination is pgsql
            ## If True, the script will keep the json/jsonb columns intact (won't be stringified), and the datetime columns will be kept in timezone specified in the settings (instead of UTC)
        }

    ]
}
```
#### Collections

#### apis
If source is PGSQL, we need to provide a field ```tables```, which is a list of table_specifications. Table_specifications shall be in following format:
```
{
    'table_name': str,
    'bookmark': False or 'field_name' (optional, Default=False, for example - 'updated_at'),
    'primary_key': string of records (Required, if logging or syncing mode),
    'primary_key_datatype': 'str' or 'int' or 'datetime' (Required, if logging or syncing mode),
    'exclude_tables': [] (Optional, List[str] or str, list of table names to exclude from entire database),
    'cron': '* * * * * 7-19 */1 0' or 'self-managed' (Refer Notes 1),
    'partition_col': False or 'column-name' (str or list of str),
    'partition_col_format': '' (Optional, Refer Notes 3),
    'mode': 'syncing' or 'logging' or 'dumping',
    'improper_bookmarks': True/False (default = True)
    'expiry': {'days': 30, 'hours': 5} (dict, Optional, used only when is_dump = True)
}
```

If source is MongoDB, we need to provide a field ```collections```, which is a list of collection_specifications. Collection_specifications shall be in following format:
```
{
    'collection_name': '',
    'fields': {
        'field_1': 'int' (Refer Notes 2),
        'field_2': 'complex', 
        ...
    } (Optional),
    'bookmark': False or 'field_name' (optional, for example - 'updated_at'),
    'archive': "Mongodb_query" or False,
    'cron': '* * * * * 7-19 */1 0' (Refer Notes 1),
    'partition_col': False or '' name of the field (str or list of str),
    'partition_col_format': '' (Optional, Refer Notes 3),
    'expiry': {'days': 30, 'hours': 5} (dict, Optional, used only when is_dump = True),
    'mode': 'syncing' or 'logging' or 'dumping',
    'improper_bookmarks': true/false,
    'batch_size': int,
    'time_delay': int (delay between each batch migration)
}
```

## fastapi_server
(Bool): default = False. If user sets 'fastapi_server' to True, a uvicorn server is started.

## timezone
(Str): default = 'Asia/Kolkata'. Used for processing dates in given timezone, using last_run_cron_job, etc. For mongo as the source, this service saves dates in UTC timezone, and for pgsql as the source, this service saves dates without changing their timezone.

## notify
(Bool): default = False. If provided, slack_notif option is enabled, where we can provide details of the slack channels, and slack_token


# slack_notif
This is a dict type object with following details:
1. slack_token: Unique slack token for this data_migration_service application
2. channel: unique ID of the channel we want to post to

Note: We need to add the bot created for slack API to the channel we want to post to, or provide the necessary permissions.

# Notes

## 1. Writing Cron Expressions
Writing Cron expression as per guidelines at [APScheduler docs](https://apscheduler.readthedocs.io/en/3.x/modules/triggers/cron.html). Format (year, month, day, week, day_of_week, hour, minute, second)

For example: '* * * * * 7-19 */1 0' represents every minute between 7AM to 7PM.

## 2. What are the three modes of operation?
1. Dumping: When we are dumping data, snapshots of the datastore are captured at regular intervals. We maintain multiple copies of the tables
2. Logging: Logging is the mode where the data is only being added to the source table, and we assume no updations are ever performed. Only new records are migrated.
3. Syncing: Where all new records is migrated, and all updations are also mapped to destination. If a record is deleted at source, it's NOT deleted at destination

## 3. What are Bookmarks?
Once new records are available at the source, they are migrated and appended to the data at destination. 
However to check for updations in the existing records, we need to have a field in the data (for example: updated_at, update_timestamp, etc.) which changes its value to latest timestamp whenever the record is updated. 
For the purpose of maintaining such column, sometimes triggers are added at the source database. [Refer this](https://stackoverflow.com/questions/70268251/trigger-function-to-update-timestamp-attribute-when-any-value-in-the-table-is-up).
Bookmarks are needed only in case mode of operation is syncing, or mirroring. In case of logging or dumping mode, we are not checking for updates, hence there is no need of bookmarks.

## 2. Specifying data types in MongoDB fields
Only specify if the field belongs to one of the following category:
1. 'bool'
2. 'float'
3. 'int'
4. 'datetime'
Other standard types are taken care of. Lists and dictionaries are stringified. If not specified, all types are by default converted to string. By default, datetime is converted to strings in MongoDB processing.

## 3. Partition Columns formats
'int' or 'str' (Default) or 'datetime', or list of these formats for different columns.

## 4. Data Dumping

True or False. If set to true, it adds a column 'migration_snapshot_date' to data, which stores the datetime of migration. Without updation checks, it simply dumps the data into destination. If set to true, one can also partition data based on 'migration_snapshot_date'.


## Notes:
1. If fastapi server is started, then data can migrated on scheduled basis, as well as immediate basis (i.e., migrating data just once).
2. If fastapi server is not started, then data can only be migrated on immediate basis (i.e., migrating data just once). To run scheduled jobs in such cases, an external scheduler is required.
3. In case of pgsql, we can migrate all tables of database by passing 'table_name' as '*'. We can also add a list of tables to exclude them in such cases.