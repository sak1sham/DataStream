{
    "destination": {
        "destination_type": "pgsql",
        "specifications": [
            {
                "db_name": "dms",
                "password": "3y5HMs^2qy%&Kma",
                "url": "3.108.43.163",
                "username": "saksham_garg"
            },
            {
                "db_name": "dms",
                "password": "3y5HMs^2qy%&Kma",
                "url": "13.233.225.181",
                "username": "saksham_garg"
            }
        ]
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
            "batch_size": 100000,
            'strict': True,
            "cron": "self-managed",
            "mode": "dumping",
            "partition_col": "migration_snapshot_date",
            "partition_col_format": "datetime",
            "table_name": "audit_logs",
            "to_partition": True
        }
    ]
}