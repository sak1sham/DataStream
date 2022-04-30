import os
from dotenv import load_dotenv
load_dotenv()

settings = {
    'timezone': 'Asia/Kolkata',
    'notify': True,
    'slack_notif': {
        'slack_token': 'xoxb-667683339585-3192552509475-C0xJXwmmUUwrIe4FYA0pxv2N',
        'channel': "C035WQHD291"
    },
    'save_logs': False
}