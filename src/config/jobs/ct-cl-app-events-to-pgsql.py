import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "source": {
        "source_type": "api",
        "db_name": "clevertap"
    },
    "destination": {
        'redshift': {
            'host': os.getenv('REDSHIFT_URL'),
            'database': 'cmwh',
            'user': os.getenv('REDSHIFT_USER'),
            'password': os.getenv('REDSHIFT_PASSWORD'),
            'schema': 'cm_clevertap',
            's3_bucket_name': 'database-migration-service-prod',
            'destination_type': 'redshift'
        },
        # 'ec2_1': {
        #     "db_name": "cmdb",
        #     "password": os.getenv('DB_PASSWORD'),
        #     "url": "dms2.citymall.dev",
        #     "username": os.getenv('DB_USERNAME'),
        #     "schema": "public",
        #     'destination_type': 'pgsql'
        # },
        'ec2_2':  {
            "db_name": "cmdb",
            "password": os.getenv('DB_PASSWORD'),
            "url": "dms1.citymall.dev",
            "username": os.getenv('DB_USERNAME'),
            "schema": "public",
            'destination_type': 'pgsql'
        },
        # 'ec2_3': {
        #     "db_name": "cmdb",
        #     "password": os.getenv('DB_PASSWORD'),
        #     "url": "dms3.citymall.dev",
        #     "username": os.getenv('DB_USERNAME'),
        #     "destination_type": "pgsql",
        #     "schema": "public"
        # }
    },
    "apis": [
        {
            'api_name':'cl_app_events',
            'project_name': 'cl_app',
            'event_names': '*',
            'start_day': '-1',
            'end_day': '-1',
            'slack_channel':'C025PTAUUFP',
            'fields': {
                "event_name": 'str',
                "ct_ts": 'int',
                "timestamp": 'datetime',
                "name": 'str',
                "phone": 'str',
                "city": 'str',
                "user_id": 'str',
                "whatsapp_opted_in": 'str',
                "leader_id": 'str',
                "leader_uuid": 'str',
                "already_a_cx": 'str',
                "already_a_cx_wo_order":'str',
                "position": 'str',
                "is_servicable": 'str',
                "idfa": 'str',
                "version_code": 'str',
                "first_version_code": 'str',
                "catalogue_name": 'str',
                "platform": 'str',
                "ct_object_id": 'str',
                "ct_session_id": 'str',
                "screen_name": 'str',
                "os_version": 'str',
                "app_version": 'str',
                "make": 'str',
                "model": 'str',
                "tags": 'str',
                "event_props": 'str'
            },
            'lob_fields': {
                "tags": 4096,
                "event_props": 4096
            },
            'api_to_field_mapping': {
                "ct_ts": 'ts',
                "name": 'profile.name',
                "phone": 'profile.phone',
                "city": 'profile.profileData.city',
                "user_id": 'profile.profileData.user_id',
                "whatsapp_opted_in": 'profile.profileData.whatsapp_opted_in',
                "leader_id": 'profile.profileData.id',
                "leader_uuid": 'profile.profileData.leader_id',
                "already_a_cx": 'profile.profileData.already_a_cx',
                "already_a_cx_wo_order":'profile.profileData.already_a_cx_wo_order',
                "position": 'profile.profileData.position',
                "is_servicable": 'profile.profileData.is_serviceable',
                "idfa": 'profile.profileData.idfa',
                "version_code": 'profile.profileData.version_code',
                "first_version_code": 'profile.profileData.first_version_code',
                "catalogue_name": 'profile.profileData.catalogue_name',
                "platform": 'profile.platform',
                "ct_object_id": 'profile.objectId',
                "ct_session_id": 'event_props.CT Session Id',
                "screen_name": 'event_props.screen_name',
                "os_version": 'profile.os_version',
                "app_version": 'profile.app_version',
                "make": 'profile.make',
                "model": 'profile.model',
                "tags": 'profile.profileData.tags',
                "event_props": 'event_props'
            },
            'cron': 'self-managed',
        }
    ]
}