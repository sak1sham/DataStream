mapping = {
    'source': {
        'source_type': 'pgsql',
        'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
        'db_name': 'cmdb',
        'username': 'saksham_garg',
        'password': '3y5HMs^2qy%&Kma'
    },
    "destination": {
        'redshift': {
            'destination_type': 'redshift',
            'host': 'cm-redshift-1.cyl4ilkelm5m.ap-south-1.redshift.amazonaws.com',
            'database': 'cmwh',
            'user': 'cmadmin',
            'password': 'kgDzH6Zy5xZ6HHx',
            's3_bucket_name': 'database-migration-service-prod',
        }
    },
    'tables': [
        {
            'table_name': 'tbl_user',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'user_id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'lob_fields_length': {
                'location_sanity_issues': 65535,
                'tracking_info': 65535,
                'source_utm': 65535,
                'install_utm': 65535,
                'msite_tracking_info': 65535,

                'user_type': 3036,
                'referral_code': 3036,
                'version_name': 3036,
                'version_code': 3036,
                'first_version_code': 3036,
                'first_version_name': 3036,
                'codepush_version': 3036,
                'advertising_set_id': 3036,
                'advertising_set_name': 3036,
                'advertising_campaign_id': 3036,
                'advertising_objective_name': 3036,
                'advertising_id': 3036,
                'user_phone': 3036,
                'language': 3036,
                'signup_code': 3036,
                'user_image': 3036,
                'signup_source': 3036,
                'one_signal_user_id': 3036,
                'rzpay_contact_id': 3036,
                'install_google_ad_campaign': 3036,
                'advertising_partner': 3036,
                'advertising_campaign': 3036,
                'user_name': 7084,
                'image': 3036,
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
