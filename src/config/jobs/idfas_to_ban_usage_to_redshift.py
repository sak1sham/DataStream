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
        'host': 'cm-redshift-1.cyl4ilkelm5m.ap-south-1.redshift.amazonaws.com',
        'database': 'cmwh',
        'user': 'cmadmin',
        'password': 'kgDzH6Zy5xZ6HHx',
        's3_bucket_name': 'database-migration-service-prod',
    },
    'tables': [            
        {
            'table_name': 'idfas_to_ban_usage',
            'cron': 'self-managed',
            'mode': 'logging',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'batch_size': 10000,
            'lob_fields_length': {
                'idfa': 3036,
            },
            'buffer_updation_lag':{
                'hours': 2,
            },
            'grace_updation_lag': {
                'hours': 2
            },
        },
    ]
}