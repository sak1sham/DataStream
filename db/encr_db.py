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
        return collection_encr
    except:
        logging.info("Unable to connect to encryption store database.")
        return None

def get_last_run_cron_job(id):
    db = get_data_from_encr_db()
    prev = db.find_one({'last_run_cron_job_for_id': id})
    if(prev):
        ## MongoDB stores and returns all datatimes in UTC by default. We need to adjust them.
        timing = prev['timing']
        timing = timing.replace(tzinfo=IST_tz)
        logging.info(id + "Glad to see this database again!")
        db.delete_one({'last_run_cron_job_for_id': id})
        db.insert_one({'last_run_cron_job_for_id': id, 'timing': datetime.datetime.utcnow().replace(tzinfo = IST_tz)})
        return timing
    else:
        timing = IST_tz.localize(datetime.datetime(1602, 8, 20, 0, 0, 0, 0))
        logging.info(id + ": Never seen it before. Taking previous run for cron job as on August 20, 1602, IST.")
        db.insert_one({'last_run_cron_job_for_id': id, 'timing': datetime.datetime.utcnow().replace(tzinfo = IST_tz)})
        return timing