import os
from dotenv import load_dotenv
load_dotenv()
import datetime

settings = {
    'fastapi_server': False,
    'timezone': 'Asia/Kolkata',
    'notify': False,
    'encryption_store': {
        'url': os.getenv('ENCR_MONGO_URL'),
        'db_name': os.getenv('DB_NAME'),
        'collection_name': os.getenv('COLLECTION_NAME')
    },
    'dashboard_store': {
        'url': os.getenv('ENCR_MONGO_URL'),
        'db_name': os.getenv('DB_NAME'),
        'collection_name': os.getenv('DASHBOARD_COLLECTION_NAME')
    },
    'slack_notif': {
        'slack_token': os.getenv('SLACK_TOKEN'),
        'channel': os.getenv('SLACK_CHANNEL')
    },
}