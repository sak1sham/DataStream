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
            'database': 'redshift-db-name',
            'user': os.getenv('REDSHIFT_USER'),
            'password': os.getenv('REDSHIFT_PASSWORD'),
            'schema': 'cm_clevertap',
            's3_bucket_name': 's3-bucket-name',
            'destination_type': 'redshift'
        },
        'destination_1': {
            'destination_type': 'pgsql',
            "db_name": "database-name",
            "password": os.getenv('DB_PASSWORD'),
            "url": "destination.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "schema": "public",
        },
        'destination_2':  {
            'destination_type': 'pgsql',
            "db_name": "database-name",
            "password": os.getenv('DB_PASSWORD'),
            "url": "destination.connection.url",
            "username": os.getenv('DB_USERNAME'),
            "schema": "public",
        }
    },
    
    "apis": [
        {
            'api_name':'cx_app_events',
            'project_name': 'cx_app',
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
                "cx_city": 'str',
                "city": 'str',
                "user_id": 'str',
                "whatsapp_opted_in": 'str',
                "leader_id": 'str',
                "leader_name": 'str',
                "leader_user_id": 'str',
                "leader_lat":'str',
                "leader_lng": 'str',
                "catalogue_name": 'str',
                "platform": 'str',
                "ct_object_id": 'str',
                "ct_session_id": 'str',
                "screen_name": 'str',
                "os_version": 'str',
                "app_version": 'str',
                "make": 'str',
                "model": 'str',
                "cplabel": 'str',
                "tags": 'str',
                "event_props": 'str'
            },
            'lob_fields': {
                "tags": 4096,
                "event_props": 4096,
                "name": 1024
            },
            'api_to_field_mapping': {
                "ct_ts": 'ts',
                "name": 'profile.name',
                "phone": 'profile.phone',
                "cx_city": 'profile.profileData.cx_city',
                "city": 'profile.profileData.city',
                "user_id": 'profile.profileData.user_id',
                "whatsapp_opted_in": 'profile.profileData.whatsapp_opted_in',
                "leader_id": 'profile.profileData.leaderid',
                "leader_name": 'profile.profileData.leadername',
                "leader_user_id": 'profile.profileData.leaderuserid',
                "leader_lat":'profile.profileData.leaderlat',
                "leader_lng": 'profile.profileData.leaderlng',
                "catalogue_name": 'profile.profileData.catalogue_name',
                "platform": 'profile.platform',
                "ct_object_id": 'profile.objectId',
                "ct_session_id": 'event_props.CT Session Id',
                "screen_name": 'event_props.screen_name',
                "os_version": 'profile.os_version',
                "app_version": 'profile.app_version',
                "make": 'profile.make',
                "model": 'profile.model',
                "cplabel": 'profile.profileData.cplabel',
                "tags": 'profile.profileData.tags',
                "event_props": 'event_props'
            },
            'cron': 'self-managed',
        }
    ]
}