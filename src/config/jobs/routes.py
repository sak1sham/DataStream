query_1 = """select * from routes where completed_at <= '2021-05-10'::timestamp and completed_at is not null"""

mapping = { 
    'source': { 
        'source_type': 'sql', 
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com', 
        'db_name': 'cmdb', 
        'username': 'saksham_garg', 
        'password': '3y5HMs^2qy%&Kma' 
    }, 
    'destination': { 
        'destination_type': 's3', 
        's3_bucket_name': 'database-migration-service-prod'
    }, 
    # 'tables': [ 
    #     {
    #         'table_name': 'routes', 
    #         'cron': 'self-managed', 
    #         'mode': 'syncing',
    #         'primary_key': 'id',
    #         'primary_key_datatype': 'int',
    #         'to_partition': True, 
    #         'partition_col': 'started_at',
    #         'partition_col_format': 'datetime',
    #         'bookmark': 'updated_at_for_pipeline', 
    #         'improper_bookmarks': False, 
    #         'batch_size': 10000, 
    #         'buffer_updation_lag': {
    #             'hours': 2,
    #         } ,
    #         'grace_updation_lag': {
    #             'days': 1
    #         },
    #     },
    # ]
    'tables': [
        {
            'table_name': 'routes',
            'cron': 'self-managed',
            'to_partition': True,
            'partition_col': 'started_at',
            'partition_col_format': 'datetime',
            'mode': 'dumping',
            'fetch_data_query': query_1,
            'fields': {
                'id': 'int',
                'priority': 'int',
                'total_cash_collected': 'int',
                'total_upi_collected': 'int',
                'order_id': 'int',
                'delivery_date': 'datetime',
                'reached_at': 'datetime',
                'started_at': 'datetime',
                'completed_at': 'datetime',
                'missed_at': 'datetime',          
            },
            'batch_size': 10000,
        },
    ]
}