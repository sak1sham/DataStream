mapping = {
    'source': {
        'source_type': 'sql',
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'cmdb',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
    },
    'destination': {
        'destination_type': 'redshift',
        'host': 'cm-redshift-1.cyl4ilkelm5m.ap-south-1.redshift.amazonaws.com',
        'database': 'cmwh',
        'user': 'cmadmin',
        'password': 'kgDzH6Zy5xZ6HHx',
        's3_bucket_name': 'database-migration-service-prod',
    },
    'tables': [
        {
            'table_name': 'scratch_cards',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at_for_pipeline',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'lob_fields_length': {
                'ui_properties': 65535,
                'action_details': 65535,
                'campaign_name': 3036,
                'campaign_description': 3036,
                'state': 3036,
                'duplicate_sc_check': 3036,
                'campaign_type': 3036,
            },
            'grace_updation_lag': {
                'hours': 2
            },
            'buffer_updation_lag':{
                'hours': 2,
            }
        },
    ]
}