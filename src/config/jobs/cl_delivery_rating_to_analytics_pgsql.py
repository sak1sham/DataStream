mapping = {
    "destination": {
        'ec2_1': {
            "db_name": "cmdb",
            "password": "3y5HMs^2qy%&Kma",
            "url": "3.108.43.163",
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
            "bookmark": "updated_at_for_pipeline",
            "buffer_updation_lag": {
                "hours": 2
            },
            "cron": "self-managed",
            "grace_updation_lag": {
                "days": 1
            },
            "improper_bookmarks": False,
            "mode": "mirroring",
            "primary_key": "id",
            "primary_key_datatype": "int",
            "table_name": "cl_delivery_rating"
        }
    ]
}