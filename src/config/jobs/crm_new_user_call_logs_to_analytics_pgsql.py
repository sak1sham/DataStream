mapping = {
    'source': {
        'source_type': 'sql',
        'url': 'crmdb.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'crmdb',
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
            'table_name': 'user_call_logs',
            'mode': 'syncing',
            'primary_key': 'call_id',
            'primary_key_datatype': 'int',
            'cron': 'self-managed',
            'to_partition': True,
            'partition_col': 'created_at',
            'partition_col_format': 'datetime',
            'bookmark': 'updated_at_for_pipeline',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'buffer_updation_lag':{
                'hours': 2,
            },
            'grace_updation_lag': {
                'days': 1
            },
        }
    ]
}