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
            'table_name': 'tbl_admin',
            'cron': 'self-managed',
            'mode': 'syncing',
            'primary_key': 'admin_id',
            'primary_key_datatype': 'int',
            'bookmark': 'updated_at_for_pipeline',
            'improper_bookmarks': False,
            'batch_size': 10000,
            'lob_fields_length': {
                'full_name': 3036,
                'password': 3036,
                'username': 3036,
                'email_id': 3036,
                'phone_number': 3036,
                'alternate_phone': 3036,
                'department': 3036,
                'sub_department': 3036,
                'city': 3036,
                'lm_vendor': 3036,
                'roam_db_id': 3036,
                'employee_id': 3036,
                'db_last_lng': 3036,
                'bank_account_no': 3036,
                'ifsc_code': 3036,
                'bank_account_name': 3036,
                'tpl_vendor': 3036,
                'designation': 3036,
                'aadhar_number': 3036,
                'aadhar_pdf_link': 3036,
                'admin_type': 3036,
                'db_vehicle_type': 3036,
                'db_payment_model': 3036,
                'db_app_version': 3036,
                'db_last_lat': 3036,
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