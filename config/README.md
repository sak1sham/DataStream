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
    'db_name': '' (name of dabatase for mongoDB and SQL sources),
    'username': '' (Optional),
    'password': '' (Optional)
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
    'bookmark_creation': False or 'field_name' (optional, Default=False, for example - 'created_at'),
    'bookmark': False or 'field_name' (optional, Default=False, for example - 'updated_at'),
    'primary_keys': string or list of unique specifiers for records (Optional),
    'archive': "SQL_query" or False,
    'cron': '* * * * * 7-19 */1 0' (Refer Notes 1),
    'to_partition': True or False (Default),
    'partition_col': False or '' name of the column (str or list of str),
    'partition_col_format': '' (Optional, Refer Notes 3),
    'is_dump': False (Optional, Default=False, Refer Notes 4)
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
    'to_partition': True or False (Default),
    'partition_col': False or '' name of the field (str or list of str),
    'partition_col_format': '' (Optional, Refer Notes 3),
    'is_dump': False
}
```

If source is API, we need to provide a field ```API```, which is a list of api_specifications. 
Api_specifications shall be in following format:
```
{
    'api_name': '',
    'fields': {
        'field_1': 'int' (Refer Notes 3),
        'field_2': 'complex', 
        ...
    } (Optional),
    'bookmark': False or 'field_name' (optional, for example - 'updated_at'),
    'cron': '* * * * * 7-19 */1 0' (Refer Notes 1),
    'to_partition': True or False (Default),
    'partition_col': False or '' name of the field (str or list of str),
    'partition_col_format': '' (Optional, Refer Notes 4),
    'is_dump': False
}
```

# Notes

## 1. Writing Cron Expressions
Writing Cron expression as per guidelines at [APScheduler docs](https://apscheduler.readthedocs.io/en/v2.1.0/cronschedule.html). Format (year, month, day, week, day_of_week, hour, minute, second)

For example: '* * * * * 7-19 */1 0' represents every minute between 7AM to 7PM.

## 2. Specifying data types in MongoDB fields
Only specify if the field belongs to one of the following category:
1. 'bool'
2. 'float'
3. 'complex'
4. 'int'

Other standard types are taken care of. Lists and dictionaries are stringified. If not specified, all types are by default converted to string. By default, datetime is converted to strings in MongoDB processing.

## 3. Partition Columns formats

'int' or 'str' (Default) or 'datetime', or list of these formats for different columns.

## 4. Data Dumping

True or False. If set to true, it adds a column 'migration_snapshot_date' to data, which stores the datetime of migration. Without updation checks, it simply dumps the data into destination. If set to true, one can also partition data based on 'migration_snapshot_date'.