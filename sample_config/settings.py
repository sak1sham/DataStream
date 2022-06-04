import os
from dotenv import load_dotenv
load_dotenv()
import datetime
import pytz

settings = {
    'fastapi_server': False,        ## Setting FastAPI server will help in checking the health of the script by pinging at /health route. Here, we have disabled that option. To enable it, set it as True
    'timezone': 'Asia/Kolkata',     ## Timezone in which the data operates
    'notify': True,                 ## We want the script to send a notification once a migration is completed
    'encryption_store': {           ## Encryption settings are essential for the working of the script. 
        'url': os.getenv('ENCR_MONGO_URL'),
        'db_name': os.getenv('DB_NAME'),
        'collection_name': os.getenv('COLLECTION_NAME')
    },
    'dashboard_store': {            ## The details of the MongoDB collection which is used to store the history of all jobs ran till date by DMS
        'url': os.getenv('ENCR_MONGO_URL'),
        'db_name': os.getenv('DB_NAME'),
        'collection_name': 'dms_history'
    },
    'slack_notif': {                ## Slack tokens and the channel in which we want the script to send the notifications once the migration has been completed.
        'slack_token': 'x**Z-88*********7-421*********6-F0x****************xv3Z',
        'channel': "C042ANGF320"
    },
    'cut_off_time': datetime.time(hour=9, minute=0, second=0, microsecond=0, tzinfo=pytz.timezone('Asia/Kolkata')),     ## We don't want the script to run past 9 AM IST, so specified a cut-off timing after which the script will send a sigterm and stop automatically
}