from config.migration_mapping import encryption_store
from pymongo import MongoClient
import certifi
import logging
logging.getLogger().setLevel(logging.INFO)

import pytz
import datetime
IST_tz = pytz.timezone('Asia/Kolkata')

def get_data_from_encr_db():
    try:
        client_encr = MongoClient(encryption_store['url'], tlsCAFile=certifi.where())
        db_encr = client_encr[encryption_store['db_name']]
        collection_encr = db_encr[encryption_store['collection_name']]
        logging.info("Successfully connected to encryption database.")
        return collection_encr
    except:
        logging.info("Unable to connect to encryption store database.")
        return None

def get_last_run_cron_job(db, id):
    prev = db.find_one({'last_run_cron_job_for_id': id})
    if(prev):
        timing = prev['timing']
        db.delete_one({'last_run_cron_job_for_id': id})
        db.insert_one({'last_run_cron_job_for_id': id, 'timing': datetime.datetime.utcnow().replace(tzinfo = IST_tz)})
        return timing
    else:
        timing = IST_tz.localize(datetime.datetime(1602, 8, 20, 0, 0, 0, 0))
        logging.info("Never seen " + id + " before. Taking previous run for cron job as August 20, 1602.")
        db.insert_one({'last_run_cron_job_for_id': id, 'timing': datetime.datetime.utcnow().replace(tzinfo = IST_tz)})
        return timing