from config.settings import settings
from pymongo import MongoClient
from helper.exceptions import ConnectionError
from helper.logger import logger

import pytz
import datetime
from typing import NewType, Any, Dict
datetype = NewType("datetype", datetime.datetime)

encryption_store = settings['encryption_store']


def get_data_from_encr_db():
    '''
        Function to get connection to the encryption database/collection
    '''
    try:
        certificate = 'config/rds-combined-ca-bundle.pem'
        client_encr = MongoClient(encryption_store['url'], tlsCAFile=certificate)
        db_encr = client_encr[encryption_store['db_name']]
        collection_encr = db_encr[encryption_store['collection_name']]
        return collection_encr
    except:
        raise ConnectionError("Unable to connect to Encryption DB.")


def get_last_run_cron_job(job_id: str) -> datetype:
    '''
        Function to find and return the time when the job with id job_id was last run.
        If the job was never run before, it returns an old date (Jan 1, 1999)
        Datetime is returned in UTC timezone
    '''
    db = get_data_from_encr_db()
    prev = db.find_one({'last_run_cron_job_for_id': job_id})
    if(prev):
        timing = prev['timing']
        logger.inform(job_id=job_id, s=(job_id + ": Glad to see this database again!"))
        return pytz.utc.localize(timing)
    else:
        timing = datetime.datetime(1999, 1, 1, 0, 0, 0, 0, tzinfo=pytz.utc)
        logger.inform(job_id=job_id, s=(job_id + ": Never seen it before. Taking previously run cron job time as on January 1, 1999."))
        return timing


def get_last_migrated_record_prev_job(job_id: str = None) -> Any:
    '''
        Function to return last migrated record on completion of last cron job
        Returns the id of last record
    '''
    db = get_data_from_encr_db()
    prev = db.find_one({'last_run_cron_job_for_id': job_id})
    if(prev):
        return prev['record_id']
    else:
        return None


def set_last_run_cron_job(job_id: str, timing: datetype, last_record_id: Any = None) -> None:
    '''
        Function to set the time of current job. This might prove useful on running the job next time
        We can also pass a record_id of the last record migrated during the job to store it.
    '''
    db = get_data_from_encr_db()
    prev = db.find_one({'last_run_cron_job_for_id': job_id})
    rec = {
        'last_run_cron_job_for_id': job_id, 
        'timing': timing,
        'record_id': last_record_id
    }    
    if(prev):
        db.delete_one({'last_run_cron_job_for_id': job_id})
        db.insert_one(rec)
    else:
        db.insert_one(rec)


def get_last_migrated_record(job_id: str) -> Dict[str, Any]:
    '''
        After every batch is saved, we save the record_id of its last record in order to resume in case the system fails
        This function returns that record_id whenever called.
    '''
    db = get_data_from_encr_db()
    prev = db.find_one({'last_migrated_record_for_id': job_id})
    if(prev):
        prev['timing'] = pytz.utc.localize(prev['timing'])
        return prev
    else:
        return None


def set_last_migrated_record(job_id: str, _id: Any, timing: datetype) -> None:
    '''
        After every batch is saved, we save the record_id of its last record in order to resume in case the system fails
        This function saves that record_id corresponding to every job_id
    '''
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


def delete_metadata_from_mongodb(job_id: str = None) -> None:
    '''
        Delete all records from mongodb temporary data
    '''
    db = get_data_from_encr_db()
    db.delete_many({'last_migrated_record_for_id': job_id})
    db.delete_many({'last_run_cron_job_for_id': job_id})
    db.delete_many({'table': job_id})
    db.delete_many({'collection': job_id})

