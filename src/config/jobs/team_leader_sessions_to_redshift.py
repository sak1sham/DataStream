mapping = {
    'source': {
        'source_type': 'sql',
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'cmdb',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
    },
    "destination": {
        'destination_type': 'redshift',
        'specifications': [
            {
                'host': 'cm-redshift-1.cyl4ilkelm5m.ap-south-1.redshift.amazonaws.com',
                'database': 'cmwh',
                'user': 'cmadmin',
                'password': 'kgDzH6Zy5xZ6HHx',
                's3_bucket_name': 'database-migration-service-prod',
            }
        ]
    },
    'tables': [
        {
            'table_name': 'team_leader_sessions',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'session_id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'lob_fields_length': {
                'device_info': 65535,
                'idfa': 3036,
                'unique_device_id': 3036,
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