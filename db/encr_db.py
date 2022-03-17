from config.migration_mapping import settings
from pymongo import MongoClient
import certifi
from helper.exceptions import ConnectionError
from helper.logger import logger

from bson import ObjectId
import pytz
import datetime
from typing import NewType, Any, Dict
datetype = NewType("datetype", datetime.datetime)

encryption_store = settings['encryption_store']

def get_data_from_encr_db():
    try:
        client_encr = MongoClient(encryption_store['url'], tlsCAFile=certifi.where())
        db_encr = client_encr[encryption_store['db_name']]
        collection_encr = db_encr[encryption_store['collection_name']]
        return collection_encr
    except:
        raise ConnectionError("Unable to connect to Encryption DB.")


def get_last_run_cron_job(job_id: str) -> datetype:
    db = get_data_from_encr_db()
    prev = db.find_one({'last_run_cron_job_for_id': job_id})
    if(prev):
        timing = prev['timing']
        logger.inform(job_id + ": Glad to see this database again!")
        return pytz.utc.localize(timing)
    else:
        timing = datetime.datetime(1999, 1, 1, 0, 0, 0, 0, tzinfo=pytz.utc)
        logger.inform(job_id + ": Never seen it before. Taking previously run cron job time as on January 1, 1999.")
        return timing


def set_last_run_cron_job(job_id: str, timing: datetype) -> None:
    db = get_data_from_encr_db()
    prev = db.find_one({'last_run_cron_job_for_id': job_id})
    if(prev):
        db.delete_one({'last_run_cron_job_for_id': job_id})
        db.insert_one({'last_run_cron_job_for_id': job_id, 'timing': timing})
    else:
        db.insert_one({'last_run_cron_job_for_id': job_id, 'timing': timing})


def get_last_migrated_record(job_id: str) -> Dict[str, Any]:
    db = get_data_from_encr_db()
    prev = db.find_one({'last_migrated_record_for_id': job_id})
    if(prev):
        prev = pytz.utc.localize(prev['timing'])
        return prev
    else:
        prev_time = datetime.datetime(1999, 1, 1, 0, 0, 0, 0, tzinfo=pytz.utc)
        prev = {
            'last_migrated_record_for_id': job_id,
            'record_id': ObjectId(prev_time),
            'timing': prev_time,
        }
        return prev

def set_last_migrated_record(job_id: str, _id: Any, timing: datetype) -> None:
    rec = {
        'last_migrated_record_for_id': job_id,
        'record_id': _id,
        'timing': timing
    }
    db = get_data_from_encr_db()
    prev = db.find_one({'last_migrated_record_for_id': job_id})
    if(prev):
        db.delete_one({'last_migrated_record_for_id': job_id})
        db.insert_one(rec)
    else:
        db.insert_one(rec)