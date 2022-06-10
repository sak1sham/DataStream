{
    "destination": {
        "destination_type": "s3",
        "specifications": [
            {
                "s3_bucket_name": "database-migration-service-prod"
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
            "cron": "self-managed",
            "mode": "logging",
            "partition_col": "created_at",
            "partition_col_format": "datetime",
            "primary_key": "id",
            "primary_key_datatype": "int",
            "table_name": "recent_user_searches_archive",
        }
    ]
}