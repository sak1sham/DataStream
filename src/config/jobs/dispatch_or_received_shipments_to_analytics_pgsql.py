mapping = {
    'source': {
        'source_type': 'pgsql',
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'cmdb',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
    },
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
    'tables': [
        {
            'table_name': 'dispatch_or_received_shipments',
            'cron': 'self-managed',
            'mode': 'logging',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'partition_col': 'scanned_at',
            'partition_col_format': 'datetime',
            'batch_size': 100000,
            'strict': True,
            'grace_updation_lag': {
                'days': 1
            },
            'buffer_updation_lag':{
                'hours': 2,
            }
        },
    ]
}