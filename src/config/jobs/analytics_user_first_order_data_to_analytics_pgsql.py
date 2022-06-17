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
            "schema": "analytics"
        },
        'ec2_2': {
            "db_name": "cmdb",
            "password": "3y5HMs^2qy%&Kma",
            "url": "13.233.225.181",
            "username": "saksham_garg",
            "destination_type": "pgsql",
            "schema": "analytics"
        }
    },
    'tables': [            
        {
            'table_name': 'analytics.user_first_order_data',
            'cron': 'self-managed',
            'mode': 'mirroring',
            'primary_key': 'user_id',
            'primary_key_datatype': 'int',
            'partition_col': 'first_order_created_date',
            "partition_col_format": "datetime",
            'bookmark': 'updated_at_for_pipeline',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'strict': True,
            'buffer_updation_lag': {
                'hours': 2,
            } ,
            'grace_updation_lag': {
                'days': 1
            },
        },
    ]
}