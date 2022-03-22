from config.migration_mapping import settings
from pymongo import MongoClient
import certifi
from helper.exceptions import ConnectionError
from helper.logger import logger

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
        logger.inform(job_id=job_id, s=(job_id + ": Glad to see this database again!"), save=True)
        return pytz.utc.localize(timing)
    else:
        timing = datetime.datetime(1999, 1, 1, 0, 0, 0, 0, tzinfo=pytz.utc)
        logger.inform(job_id=job_id, s=(job_id + ": Never seen it before. Taking previously run cron job time as on January 1, 1999."), save=True)
        return timing

def get_last_migrated_record_prev_job(job_id: str = None) -> None:
    '''
        get last migrated record when job was run last time
    '''
    db = get_data_from_encr_db()
    prev = db.find_one({'last_run_cron_job_for_id': job_id})
    return prev['record_id']

def set_last_run_cron_job(job_id: str, timing: datetype, last_record_id: Any = None) -> None:
    db = get_data_from_encr_db()
    prev = db.find_one({'last_run_cron_job_for_id': job_id})
    rec = {
        'last_run_cron_job_for_id': job_id, 
        'timing': timing
    }
    if(last_record_id):
        rec['record_id'] = last_record_id
    
    if(prev):
        db.delete_one({'last_run_cron_job_for_id': job_id})
        db.insert_one(rec)
    else:
        db.insert_one(rec)


def get_last_migrated_record(job_id: str) -> Dict[str, Any]:
    db = get_data_from_encr_db()
    prev = db.find_one({'last_migrated_record_for_id': job_id})
    if(prev):
        prev['timing'] = pytz.utc.localize(prev['timing'])
        return prev
    else:
        return None

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


def delete_last_migrated_record(job_id: str):
    '''
        Delete the last migrated record if entire batch is processed successfully
    '''
    db = get_data_from_encr_db()
    db.delete_one({'last_migrated_record_for_id': job_id})


