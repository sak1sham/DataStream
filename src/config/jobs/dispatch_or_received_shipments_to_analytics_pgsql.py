mapping = {
    'source': {
        'source_type': 'sql',
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'cmdb',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
    },
    'destination': {
        'destination_type': 'pgsql',
        'url': '3.108.43.163',
        'db_name': 'dms',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
    },
    'tables': [
        {
            'table_name': 'dispatch_or_received_shipments',
            'cron': '* * * * * */1 0 0',
            'mode': 'logging',
            'primary_key': 'shipment_id',
            'primary_key_datatype': 'int',
            'batch_size': 10000,
            'grace_updation_lag': {
                'hours': 4
            },
            'buffer_updation_lag':{
                'hours': 2,
            }
        },
    ]
}