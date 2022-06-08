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
        "db_name": "wmsdb",
        "password": "3y5HMs^2qy%&Kma",
        "source_type": "sql",
        "url": "cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com",
        "username": "saksham_garg"
    },
    "tables": [
        {
            "batch_size": 100000,
            "cron": "self-managed",
            "mode": "dumping",
            "partition_col": "created_at",
            "partition_col_format": "datetime",
            "table_name": "rm_skus_area_wise_with_mrp_snapshot",
            "to_partition": True
        }
    ]
}