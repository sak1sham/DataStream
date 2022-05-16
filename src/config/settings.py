import os
from dotenv import load_dotenv
load_dotenv()

settings = {
    'fastapi_server': True,
    'timezone': 'Asia/Kolkata',
    'notify': True,
    'encryption_store': {
        'url': os.getenv('ENCR_MONGO_URL'),
        'db_name': os.getenv('DB_NAME'),
        'collection_name': os.getenv('COLLECTION_NAME')
    },
    'logging_store': {
        'url': os.getenv('LOG_MONGO_URL'),
        'db_name': os.getenv('LOG_DB_NAME'),
        'collection_name': os.getenv('LOG_COLLECTION_NAME')
    },
    'slack_notif': {
        'slack_token': 'xoxb-667683339585-3192552509475-C0xJXwmmUUwrIe4FYA0pxv2N',
        'channel': "C035WQHD291"
    },
    'save_logs': False
}