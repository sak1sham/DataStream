mapping = {
    'source': {
        'source_type': 'sql',
        'url': 'localhost',
        'db_name': 'postgres',
    },
    "destination": {
        'destination_type': 'redshift',
        'host': 'cm-redshift-1.cyl4ilkelm5m.ap-south-1.redshift.amazonaws.com',
        'database': 'cmwh',
        'user': 'cmadmin',
        'password': 'kgDzH6Zy5xZ6HHx',
        's3_bucket_name': 'data-migration-service-dev',
    },
    'tables': [
        {
            'table_name': 'phonebook',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'lastname',
            'primary_key_datatype': 'str',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
        },
    ]
}
