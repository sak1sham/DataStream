import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "destination": {
        'ec2_1': {
            "db_name": "cmdb",
            "schema": "wms",
            "password": os.getenv('DB_PASSWORD'),
            "url": "dms2.citymall.dev",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
        },
        'ec2_2': {
            "db_name": "cmdb",
            "schema": "wms",
            "password": os.getenv('DB_PASSWORD'),
            "url": "dms1.citymall.dev",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql",
        },
        'ec2_3': {
            "db_name": "cmdb",
            "schema": "wms",
            "password": os.getenv('DB_PASSWORD'),
            "url": "dms3.citymall.dev",
            "username": os.getenv('DB_USERNAME'),
            "destination_type": "pgsql"            
        }
    },
    "source": {
        "db_name": "wmsdb",
        "password": os.getenv('DB_PASSWORD'),
        "source_type": "pgsql",
        "url": os.getenv('CMDB_URL'),
        "username": os.getenv('DB_USERNAME')
    },
    "tables": [
        {
            "batch_size": 100000,
            'strict': True,
            "cron": "self-managed",
            "mode": "logging",
            "partition_col": "timestamp",
            "partition_col_format": "datetime",
            "primary_key": "command_serial",
            "primary_key_datatype": "int",
            "table_name": "inventory_engine_commands",
        }
    ]
}