mapping = {
    "destination": {
        'ec2_1': {
            "db_name": "cmdb",
            "password": "3y5HMs^2qy%&Kma",
            "url": "15.206.171.84",
            "username": "saksham_garg",
            "destination_type": "pgsql",
            "schema": "public"
        },
        'ec2_2': {
            "db_name": "cmdb",
            "password": "3y5HMs^2qy%&Kma",
            "url": "13.233.225.181",
            "username": "saksham_garg",
            "destination_type": "pgsql",
            "schema": "public"
        }
    },
    "source": {
        "db_name": "cmdb",
        "password": "3y5HMs^2qy%&Kma",
        "source_type": "pgsql",
        "url": "cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com",
        "username": "saksham_garg"
    },
    "tables": [
        {
            "batch_size": 10000,
            'strict': True,
            "cron": "self-managed",
            "mode": "dumping",
            "partition_col": "migration_snapshot_date",
            "partition_col_format": "datetime",
            "table_name": "branch_io_imports_archive",
        }
    ]
}