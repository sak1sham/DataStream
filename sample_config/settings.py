import os
from dotenv import load_dotenv
load_dotenv()
import datetime
import pytz

settings = {
    'fastapi_server': False,
    'timezone': 'Asia/Kolkata',
    'notify': True,
    'encryption_store': {
        'url': os.getenv('ENCR_MONGO_URL'),
        'db_name': os.getenv('DB_NAME'),
        'collection_name': os.getenv('COLLECTION_NAME')
    },
    'dashboard_store': {
        'url': os.getenv('ENCR_MONGO_URL'),
        'db_name': os.getenv('DB_NAME'),
        'collection_name': 'dms_history'
    },
    'slack_notif': {
        'slack_token': 'x**Z-88*********7-421*********6-F0x****************xv3Z',
        'channel': "C042ANGF320"
    },
    'cut_off_time': datetime.time(hour=9, minute=0, second=0, microsecond=0, tzinfo=pytz.timezone('Asia/Kolkata')),
}