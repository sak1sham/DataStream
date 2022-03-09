from config.migration_mapping import settings
from pymongo import MongoClient
import certifi

import logging
logging.getLogger().setLevel(logging.INFO)

import pytz
import datetime

encryption_store = settings['encryption_store']

class ConnectionError(Exception):
    pass

def get_data_from_encr_db():
    try:
        client_encr = MongoClient(encryption_store['url'], tlsCAFile=certifi.where())
        db_encr = client_encr[encryption_store['db_name']]
        collection_encr = db_encr[encryption_store['collection_name']]
        return collection_encr
    except:
        raise ConnectionError("Unable to connect to Encryption DB.")

def get_last_run_cron_job(id: str):
    db = get_data_from_encr_db()
    prev = db.find_one({'last_run_cron_job_for_id': id})
    if(prev):
        timing = prev['timing']
        logging.info(id + ": Glad to see this database again!")
        db.delete_one({'last_run_cron_job_for_id': id})
        db.insert_one({'last_run_cron_job_for_id': id, 'timing': datetime.datetime.utcnow()})
        return timing
    else:
        timing = datetime.datetime(1999, 1, 1, 0, 0, 0, 0, tzinfo=pytz.utc)
        logging.info(id + ": Never seen it before. Taking previously run cron job time as on January 1, 1999.")
        db.insert_one({'last_run_cron_job_for_id': id, 'timing': datetime.datetime.utcnow()})
        return timing