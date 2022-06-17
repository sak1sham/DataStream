mapping = {
    "destination": {
        'ec2_1': {
            "db_name": "wmsdb",
            "schema": "public",
            "password": "3y5HMs^2qy%&Kma",
            "url": "15.206.171.84",
            "username": "saksham_garg",
            "destination_type": "pgsql",
        },
        'ec2_2': {
            "db_name": "wmsdb",
            "schema": "public",
            "password": "3y5HMs^2qy%&Kma",
            "url": "13.233.225.181",
            "username": "saksham_garg",
            "destination_type": "pgsql",
        }
    },
    "source": {
        "db_name": "wmsdb",
        "password": "3y5HMs^2qy%&Kma",
        "source_type": "pgsql",
        "url": "cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com",
        "username": "saksham_garg"
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