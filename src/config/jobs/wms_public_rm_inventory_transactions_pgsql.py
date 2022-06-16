mapping = { 
    'source': { 
        'source_type': 'pgsql', 
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com', 
        'db_name': 'wmsdb', 
        'username': 'saksham_garg', 
        'password': '3y5HMs^2qy%&Kma' 
    }, 
    "destination": {
        'ec2_1': {
            "db_name": "dms",
            "password": "3y5HMs^2qy%&Kma",
            "url": "3.108.43.163",
            "username": "saksham_garg",
            "destination_type": "pgsql",
        },
        'ec2_2': {
            "db_name": "dms",
            "password": "3y5HMs^2qy%&Kma",
            "url": "13.233.225.181",
            "username": "saksham_garg",
            "destination_type": "pgsql",
        }
    },
    'tables': [ 
        {
            'table_name': 'rm_inventory_transactions', 
            'cron': 'self-managed', 
            'mode': 'mirroring',
            'primary_key': 'inventory_transaction_id',
            'primary_key_datatype': 'uuid', 
            'partition_col': 'created_at',
            'partition_col_format': 'datetime',
            'bookmark': 'updated_at_for_pipeline', 
            'improper_bookmarks': False, 
            'batch_size': 100000,
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
