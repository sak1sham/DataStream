mapping = {
    'source': {
        'source_type': 'sql',
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'cmdb',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
    },
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
    'tables': [
        {
            'table_name': 'routes',
            'cron': 'self-managed',
            'mode': 'syncing',
            'to_partition': True, 
            'partition_col': 'started_at',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at_for_pipeline',
            'improper_bookmarks': False,
            'batch_size': 1000,
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