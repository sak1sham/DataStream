import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "source": {
        "db_name": "source_database_name",
        "password": os.getenv('DB_PASSWORD'),
        "source_type": "pgsql",
        "url": os.getenv('SOURCE_DB_URL'),
        "username": os.getenv('DB_USERNAME')
    },
    "destination": {
        'pgsql_1': {
            "db_name": "destination_database_name",
            "password": os.getenv('DB_PASSWORD'),
            "url": "pgsql_1.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
            "schema": "public"
        },
        'pgsql_2': {
            "db_name": "destination_database_name",
            "password": os.getenv('DB_PASSWORD'),
            "url": "pgsql_2.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
            "schema": "public"
        },
        'pgsql_3': {
            "db_name": "destination_database_name",
            "password": os.getenv('DB_PASSWORD'),
            "url": "pgsql_3.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
            "schema": "public"
        },
        's3_athena': {
            'destination_type': 's3', 
            's3_bucket_name': 's3-bucket-name' 
        },
        'redshift': {
            'destination_type': 'redshift',
            'host': os.getenv('REDSHIFT_URL'),
            'database': 'REDSHIFT_DB_NAME',
            'user': os.getenv('REDSHIFT_USER'),
            'password': os.getenv('REDSHIFT_PASSWORD'),
            's3_bucket_name': 's3-bucket-name',
        }
    },
    "tables": [
        {
            "table_name": "my_table",
            "mode": "mirroring",
            "cron": "self-managed",
            "batch_size": 10000,
            "bookmark": "updated_at",
            "improper_bookmarks": False,
            "primary_key": "id",
            "primary_key_datatype": "uuid",
            "buffer_updation_lag": {
                "hours": 2
            },
            "grace_updation_lag": {
                "days": 1
            },
            'strict': True,
        }
    ]
}