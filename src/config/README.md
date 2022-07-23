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
    'source_type': 'mongo', ## str: Required, can be pgsql, mongo, api, or kafka,
    'url': 'my.connection.url',  ## str: Required, url for the data source
    'db_name': 'my-db-name',  ## str: Required, name of source dabatase. In case of kafka, a dummy db_name can be provided
    'username': '', ## str: Optional, username to connect to the db
    'password': ''  ## str: Optional, password to connect to the db
},
```

### Destination
```python
'destination': {        ## Zero or more destinations can be provided, as key-value pairs of unique-names and respective destination specifications
    "dest-1": {         ## First destination. 'dest-1' is a user-defined and unique name for the destination
        'destination_type': 's3',   ## str: Required, type of destination (from s3, redshift, pgsql)
        's3_bucket_name': 'my-s3-bucket-name',  ## str: Required when destination_type is 's3' otherwise optional, name of s3-bucket which will be used to store data at destination
        's3_suffix': 'my_suffix'    ## str: Optional, the suffix to be added to name of table (Athena, and S3 folder) at destination
    },
    "my_dest_2": {      ## Second destination. 'my_dest_2' is a user-defined and unique name for the destination
        'destination_type': 'redshift',   ## str: Required, type of destination (from s3, redshift, pgsql)
        'host': 'examplecluster.abc123xyz789.us-west-1.redshift.amazonaws.com',  ## str: Required, host url to connect to redshift
        'database': 'redshift-db-name',     ## str: Required, database to connect in redshift
        'user': os.getenv('REDSHIFT_USER'), ## str: Required, username credentials for redshift connection
        'password': os.getenv('REDSHIFT_PASSWORD'), ## str: Required, password credentials for redshift connection
        's3_bucket_name': 's3-bucket-name', ## str: Required, for faster migration, the data is first saved to s3 bucket and then transferred to redshift. No data will be stored here, but is required as a middleware
    }
    'pgsql_destination_3': {    ## Third destination. 'pgsql_destination_3' is a user-defined and unique name for the destination
        "destination_type": "pgsql",    ## str: Required, type of destination (from s3, redshift, pgsql)
        "url": "destination.connection.url",    ## str: Required, host url to connect to destination
        "db_name": "database-name",     ## str: Required, database to connect in destination pgsql
        "username": os.getenv('DB_USERNAME'),   ## str: Required, credentials for destination connection
        "password": os.getenv('DB_PASSWORD'),   ## str: Required, credentials for destination connection
        "schema": "public"  ## str: Optional, schema to migrate to at destination. If not provided, data is migrated to a new schema: {source_name}_{source_db_name}_dms. For example - if schema is not provided here, the data will be migrated to mongo_my-db-name_dms
    },
},
```

### Data structuring (tables or collections or apis)
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

## 5. What are Bookmarks?
However, to maintain that sync, we need to have bookmarks. There are 2 types of bookmarks: for creation and for updation of record. Bookmark for creation specify the timestamp of when the record was created and bookmark for updation specifies the updation time for the record. That's how we can be able to sync the tables.
The updation and creation bookmark can be the same in case updation is never performed in the tables, and the records are just added.

## 6. What are the three modes of operation?
1. Dumping: When we are dumping data, snapshots of the datastore are captured at regular intervals. We maintain multiple copies of the tables
2. Logging: Logging is the mode where the data is only being added to the source table, and we assume no updations are ever performed. Only new records are migrated.
3. Syncing: Where all new records is migrated, and all updations are also mapped to destination. If a record is deleted at source, it's NOT deleted at destination

## Notes:
1. If fastapi server is started, then data can migrated on scheduled basis, as well as immediate basis (i.e., migrating data just once).
2. If fastapi server is not started, then data can only be migrated on immediate basis (i.e., migrating data just once). To run scheduled jobs in such cases, an external scheduler is required.
3. In case of pgsql, we can migrate all tables of database by passing 'table_name' as '*'. We can also add a list of tables to exclude them in such cases.