mapping = {
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
            "batch_size": 10000,
            "cron": "self-managed",
            "mode": "dumping",
            "table_name": "day_wise_tp_wise_exhausted_capacity"
        }
    ]
}