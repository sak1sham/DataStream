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
            'table_name': 'rto_reconciliation',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'rto_recon_id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at_for_pipeline',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'col_rename': {
                'tag': 'tag_'
            },
            'buffer_updation_lag':{
                'hours': 2,
            },
            'grace_updation_lag': {
                'days': 1
            },
        },
    ]
}