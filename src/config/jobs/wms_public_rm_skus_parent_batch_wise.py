mapping = { 
    'source': { 
        'source_type': 'pgsql', 
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com', 
        'db_name': 'wmsdb', 
        'username': 'saksham_garg', 
        'password': '3y5HMs^2qy%&Kma' 
    }, 
    'destination': { 
        's3': {
            'destination_type': 's3', 
            's3_bucket_name': 'database-migration-service-prod' 
        }
    },
    'tables': [ 
        {
            'table_name': 'rm_skus_parent_batch_wise', 
            'cron': 'self-managed', 
            'mode': 'syncing',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'partition_col': 'grn_date',
            'partition_col_format': 'datetime',
            'bookmark': 'updated_at_for_pipeline', 
            'improper_bookmarks': False, 
            'batch_size': 10000, 
            'buffer_updation_lag': {
                'hours': 2,
            } ,
            'grace_updation_lag': {
                'days': 1
            },
        },
    ]
}
