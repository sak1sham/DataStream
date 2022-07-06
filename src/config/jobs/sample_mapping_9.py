query_1 = '''
SELECT weather.city, weather.temp_lo, weather.temp_hi,
       weather.prcp, weather.date, cities.location
FROM weather JOIN cities ON weather.city = cities.name;
'''

import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    'source': {
        'source_type': 'pgsql',
        'url': os.getenv('SOURCE_DB_URL'),
        "db_name": "database-name",
        'username': os.getenv('DB_USERNAME'),
        'password': os.getenv('DB_PASSWORD')
    },
    'destination': { 
        's3': {
            'destination_type': 's3', 
            's3_bucket_name': 's3-bucket-name',
            's3_suffix': '_dumped'
        }
    },
    'tables': [
        {
            'table_name': 'name_of_my_table',
            'cron': 'self-managed',
            'partition_col': 'migration_snapshot_date',
            'partition_col_format': 'datetime',
            'mode': 'dumping',
            'fetch_data_query': query_1,
            'fields': {
                'city': 'str',
                'sellable_quantity': 'int',
                'temp_lo': 'bool',
                'temp_hi': 'bool',
                'prcp': 'float',
                'date': 'datetime',
                'location': 'str',
            },
            'batch_size': 10000,
        },
    ]
}
