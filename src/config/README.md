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
4. Whenever kafka is a source, an additional key "redis" needs to be provided to temporarily cache data

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
For MongoDB and PgSQL as sources
```python
'source': {
    'source_type': 'mongo', 
    ## str: Required, can be pgsql, mongo
    
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

For Kafka as source
```python
'source': {
    'source_type': 'kafka', 
    ## str: Required
    
    'kafka_server': 'my.connection.url',  
    ## str: Required, server for kafka connection
    
    'db_name': 'my-db-name',  
    ## str: Required, A dummy database name, required to make a schema at destination
    
    'kafka_username': '', 
    ## str: Optional, username to connect to the kafka server
    
    'kafka_password': '',
    ## str: Optional, password to connect to the kafka server

    'consumer_group_id': "you_consumer_group_id"
    ## str: Required to consume data from source's topic
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
            ## Jobs can be scheduled only when fastapi server is enabled in settings. Otherwise, an external scheduler can be used (like kubernetes) with fastapi server disabled and cron set to 'self-managed'

            "mode": "mirroring",
            ## str: Required, Mode of migration: dumping, logging, syncing, or mirroring
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
If source type is Mongo, then the "collections" specification needs to be provided to identify the collection characteristics at source

"collections": List[Dict[str, Any]]
```python
mapping = {
    'source': {
        'source_type': 'pgsql'
        ...
    },
    'destination': {...},
    'collections': [
        ## collections is a list. Each element of this list corresponds to a collection specification (dictionary)
        ## Here just a single collection is specified for the sake of simplicity. Refer jobs\sample_mapping_2.py for specifying multiple collections
        {
            ## collection 1
            "collection_name": "my_collection",
            ## str: Required, Name of the collection at source

            "cron": "self-managed",
            ## str: Required: "self-managed" or a specific cron-schedule like '* * * * * 7-19 */1 0' (Refer Notes 1)
            ## If cron is self-managed, the script will start immediately, without any internal scheduler.
            ## If a specific cron is provided (As per Notes 1), the script will use APScheduler to interally schedule the migration
            ## Jobs can be scheduled only when fastapi server is enabled in settings. Otherwise, an external scheduler can be used (like kubernetes) with fastapi server disabled and cron set to 'self-managed'

            "mode": "syncing",
            ## str: Required, Mode of migration: dumping, logging, or syncing
            ## Refer Notes 2 for understanding the three modes of operations.
            
            "batch_size": 10000,
            ## int: Optional, the size of the batch/chunks. Default=10000

            'time_delay': 2 
            ## int: Optional. Seconds to delay between migration of each batch migration
            
            "bookmark": "updated_at",
            ## str: Required if mode is syncing, otherwise Optional
            ## Refer Notes 3 for understanding what are bookmarks
            
            "improper_bookmarks": False,
            ## bool: Optional (Default = False)
            ## set this as true in case the bookmark fields at source have datetimes in any other standard format (like string)
            
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
            
            "fields": {
                'created_at': 'datetime',
                'is_available': 'bool',
                'priority': 'int',
                'amount': 'float',
                ...
            }
            ## Dict[str, str]: Optional. By default all fields are considered as strings, and get stringified
            ## field-type can be datetime, bool, int or float
            ## If source is not present in the specified datatype, an attempt is made to type-cast data
            ## If type-casting is not possible, default values (int: 0, float: null, datetime: pd.NaT, bool: null) are provided
        }
    ]
}
```

#### Topics
If source type is Kafka, then the "topics" specification needs to be provided to identify the incoming topic characteristics. Sample mapping can be referred to [here](jobs\sample_mapping_18.py)

"topics": List[Dict[str, Any]] 
```python
mapping = {
    'source': {
        'source_type': 'pgsql'
        ...
    },
    'destination': {...},
    'topics': [
        ## topics is a list. Each element of this list corresponds to a topic specification (dictionary)
        ## Here just a single topic is specified for the sake of simplicity
        {
            ## topic 1
            "topic_name": "my_topic",
            ## str: Required, Name of the topic at source

            "cron": "self-managed",
            ## str: Required: "self-managed" or a specific cron-schedule like '* * * * * 7-19 */1 0' (Refer Notes 1)
            ## If cron is self-managed, the script will start immediately, without any internal scheduler.
            ## If a specific cron is provided (As per Notes 1), the script will use APScheduler to interally schedule the migration
            ## Jobs can be scheduled only when fastapi server is enabled in settings. Otherwise, an external scheduler can be used (like kubernetes) with fastapi server disabled and cron set to 'self-managed'

            "batch_size": 10000,
            ## int: Optional, the size of the batch/chunks. Default=10000

            'partition_col': 'created_at',
            ## str or False: Optional (Default=False). Useful when destination is pgsql or s3.
            ## If False, the data won't be partitioned at destination
            ## Otherwise, data partitioning will be performed. 
            ## For destionation is pgsql, only datetime partitions are useful, and the monthly partitions will be created from 2015-2030

            'partition_col_format': 'datetime',
            ## str: Optional (Default = str): can be datetime, int, or str
            ## The datatype of the partition column at source
            
            "fields": {
                'created_at': 'datetime',
                'is_available': 'bool',
                'priority': 'int',
                'amount': 'float',
                ...
            }
            ## Dict[str, str]: Optional. By default all fields are considered as strings, and get stringified
            ## field-type can be datetime, bool, int or float
            ## If source is not present in the specified datatype, an attempt is made to type-cast data
            ## If type-casting is not possible, default values (int: 0, float: null, datetime: pd.NaT, bool: null) are provided

            'col_rename': {
                'field_x': 'new_field_x',
                'my_field': 'my_new_field,
                ...
            }
            ## Dict[str, str]: Optional
            ## 1-1 mapping for the fields/columns which need to be renamed at destination
        }
    ],

    ## In addition to source, destination, topics we also REQUIRE 'redis' when kafka is a source
    ## This is required to do cache the records in redis memory until a batch size is reached
    ## and then migrate the full batch, together to the destination
    ## This is significantly faster than migrating every record immediately and independently on consumption 
    'redis': {
        'url': 'redis-url',
        ## str: Required, URL of the redis db to connect to

        'password': 'redis-password'
        ## str: Required, password credentials
    }
}
```

In addition to the mapping, we also need to specify 2 functions: ```get_table_name(record)``` and ```process_dict(record)```. 
As function name says, ```get_table_name``` returns the name of the destination table for a given record. In case only a single table is maintained at destination, a constant string can be returned.
```process_dict``` function allows us to customize the record as per requirements. In case the data has to be saved without modifications, then the record can be returned as is.

# Notes

## 1. Writing Cron Expressions
Writing Cron expression as per guidelines at [APScheduler docs](https://apscheduler.readthedocs.io/en/3.x/modules/triggers/cron.html). Format (year, month, day, week, day_of_week, hour, minute, second)

For example: '* * * * * 7-19 */1 0' represents every minute between 7AM to 7PM.

## 2. What are the three modes of operation?
1. Dumping: When we are dumping data, snapshots of the datastore are captured every time and migrated to destination. Thus, we maintain multiple copies of the source data. This mode of operation adds a column "migration_snapshot_date" which is a timestamp of when the migration was done for a particular record.
2. Logging: In this mode of operation, we migrate new records from source to the destination. The existing records are not updated even when they are updated at source. We assume that data will never be updated at source. If data is deleted at source, it won't be deleted at destination
3. Syncing: Check all new records, as well as update existing ones. If data is deleted at source, it won't be deleted at destination
3. Mirroring: Check all new records, update existing ones, as well as delete non-existing ones. A complete mirror image of the source data is maintained at destination. This mode is not available in S3 destination.

## 3. What are Bookmarks?
Once new records are available at the source, they are migrated and appended to the data at destination. 
However to check for updations in the existing records, we need to have a field in the data (for example: updated_at, update_timestamp, etc.) which changes its value to latest timestamp whenever the record is updated. 
For the purpose of maintaining such column, sometimes triggers are added at the source database. [Refer this](https://stackoverflow.com/questions/70268251/trigger-function-to-update-timestamp-attribute-when-any-value-in-the-table-is-up).
Bookmarks are needed only in case mode of operation is syncing, or mirroring. In case of logging or dumping mode, we are not checking for updates, hence there is no need of bookmarks.
