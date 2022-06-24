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
            'api_name':'cx_web_events',
            'project_name': 'cx_web',
            'event_names': '*',
            'start_day': '-1',
            'end_day': '-1',
            'slack_channel':'C025PTAUUFP',
            'fields': {
                "event_name": 'str',
                "ct_ts": 'int',
                "timestamp": 'datetime',
                "session_source": 'str',
                "visitor_id": 'str',
                "slug": 'str',
                "event_props": 'str'
            },
            'lob_fields': {
                "slug": 1024,
                "event_props": 4096
            },
            'api_to_field_mapping': {
                "ct_ts": 'ts',
                "session_source": 'session_props.session_source',
                "visitor_id": 'event_props.visitorId',
                "slug": 'event_props.slug',
                "event_props": 'event_props'
            },
            'cron': 'self-managed',
        }
    ]
}