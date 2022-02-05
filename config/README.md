# How to write your own Migration Mapping

Migration mapping is a list of specifications for each pipeline. Each specification consist of source, destination and data_properties. Each specification is structured in following format:

1. No need to change the encryption_store variable
2. No need to remove any imported libraries
3. Use datetime

## Specifying pipeline_properties

### Source
```
'source': {
    'source_type': 'mongo' or 'mysql' or 'api',
    'url': '' (the url to connect to the data source)
    'db_name': '' (name of dabatase for mongoDB and SQL sources)
},
```

### Destination
```
'destination': {
    'destination_type': 's3' or 'redshift',
    's3_bucket_name': ''
},
```

### Data structure (Table or Collection or JSON)
If source is SQL, we need to provide a field ```tables```, which is a list of table_specifications. Table_specifications shall be in following format:
```
{
    'table_name': str,
    'bookmark_creation': False or 'field_name' (optional, for example - 'created_at'),
    'bookmark_creation_format': '' (optional, Refer Notes 2),
    'bookmark': False or 'field_name' (optional, for example - 'updated_at'),
    'bookmark_format': '' (optional, Refer Notes 2),
    'uniqueness': string or list of unique specifiers for records (Optional),
    'archive': "SQL_query" or False,
    'cron': '* * * * * 7-19 */1 0' (Refer Notes 1),
    'to_partition': True or False (Default),
    'partition_col': False or '' name of the datetime column,
    'partition_col_format': '' (Optional, Refer Notes 2),
    'last_run_cron_job': Datetime object with a specified timezone (Optional)
}
```

If source is MongoDB, we need to provide a field ```collections```, which is a list of collection_specifications. Collection_specifications shall be in following format:
```
{
    'collection_name': '',
    'fields': {
        'field_1': 'integer' or 'string'
        ...
    },
    'bookmark': False or 'field_name' (optional, for example - 'updated_at'),
    'bookmark_format': '' (optional, Refer Notes 2),
    'archive': "Mongodb_query" or False,
    'cron': '* * * * * 7-19 */1 0' (Refer Notes 1),
    'to_partition': True or False (Default),
    'partition_col': False or '' name of the datetime column,
    'partition_col_format': '' (Optional, Refer Notes 2),
    'last_run_cron_job': Datetime object with a specified timezone (Optional)
}
```

# Notes

## 1. Writing Cron Expressions
Writing Cron expression as per guidelines at [APScheduler docs](https://apscheduler.readthedocs.io/en/v2.1.0/cronschedule.html). Format (year, month, day, week, day_of_week, hour, minute, second)

For example: '* * * * * 7-19 */1 0' represents every minute between 7AM to 7PM.

## 2. Writing data_formats
Date formats shall be written in pythonic way. Refer [this link](https://www.tutorialspoint.com/python/time_strptime.htm)

for example- "%Y-%m-%dT%H:%M:%S.%fZ" for dates like "2021-12-06T10:33:22Z"