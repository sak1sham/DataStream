# Settings

```fastapi_server```: (bool) Whether the fastapi server needs to be kept running in background. Jobs can be scheduled only if fastapi_server is set to True. Default=False

```timezone```: (str) the default timezone that DMS script considers. As per pytz specifications. Default='Asia/Kolkata'.

```notify```: (bool) Whether slack notifications need to be setup. Notified after completion of any migration, or when migration stops due to some exception. Default=False

```encryption_store```: (Dict[str, str]) Required. Connection to a MongoDB type database. All information about running jobs (example - last run time, last primary key inserted, etc.) is stored here. ```url```, ```db_name```, ```collection_name``` needs to be set as keys within this dictionary.

```dashboard_store```: (Dict[str, str]) Required. Connection to a MongoDB type database. All information about (un)finished jobs (example - total records migrated, storage size, time taken, etc.) is stored here. ```url```, ```db_name```, ```collection_name``` needs to be set as keys within this dictionary. The information stored in this location can help in creating a visibility dashboard for all running jobs.

```slack_notif```: (Dict[str, str]). Credentials for slack notifications. Need to be provided in case slack_notify is set to True. All information about finished jobs are notified here. The unfinished jobs are also notified with a properly formatted exception message. ```slack_token```, ```channel``` needs to be set as keys within this dictionary. Default={}

```cut_off_time```: (datetime.time, without timezone) If provided, will shut down the running jobs once the current time reaches cutoff time. Default=None

Migration mapping is a dict of specifications for each pipeline. Each specification consist of source, destination and data_properties. Each specification is structured in following format:
```
{
    "job_1_unique_id": pipeline_format_1,
    "job_2_unique_id": pipeline_format_2,
    .
    .
    .
    "job_n_unique_id": pipeline_format_n
    "fastapi_server": True (Bool, Optional, Default=False)
}
```

Note:
1. No need to change the encryption_store variable
2. No need to remove any imported libraries
3. Unique_id can't be "fastapi_server". It is a reserved keyword.

## Specifying pipeline_format
pipeline_format is a dictionary with following properties:
1. source
2. destination
3. tables, or collections or api as per source['source_type'] (Data structuring)
4. timezone

### Source
```
'source': {
    'source_type': 'mongo' or 'pgsql' or 'api', or 'kafka',
    'url': '' (the url to connect to the data source)
    'db_name': '' (name of dabatase for mongoDB and pgsql sources),
    'username': '' (Optional),
    'password': '' (Optional)
},
```

### Destination
```
'destination': {
    'destination_type': 's3' or 'redshift',
    'host': '',
    'database': '',
    'user': '',
    'password': '',
    's3_bucket_name': '',
    'schema': ''
},
```

1. destination_type : str, required, 's3' or 'redshift'
2. host : str, connection endpoint with destination, example - 'examplecluster.abc123xyz789.us-west-1.redshift.amazonaws.com' for Redshift connection
3. database : str, database name for destination
4. user : str, username to access destination storage
5. password : str, password corresponding to user to access destination storage
6. s3_bucket_name : str, name of the s3 bucket
7. schema : str, name of the schema to upload the data to

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