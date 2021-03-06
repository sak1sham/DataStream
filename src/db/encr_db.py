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
    except Exception as e:
        raise ConnectionError("Unable to connect to Encryption DB.") from e


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
        logger.inform(s = f"{job_id}: Glad to see this database again!")
        return pytz.utc.localize(timing)
    else:
        timing = pytz.utc.localize(datetime.datetime(1999, 1, 1, 0, 0, 0, 0))
        logger.inform(s = f"{job_id}: Never seen it before. Taking previously run cron job time as on January 1, 1999.")
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
    db.delete_many({'recovery_record_for_id': job_id})
    db.delete_many({'table': job_id})
    db.delete_many({'collection': job_id})


def save_recovery_data(job_id: str = None, _id: Any = None, timing: datetime.datetime = None) -> None:
    '''
        On encountering any error, we try to save the last migrated record before error in the failed job
    '''
    rec = {
        'recovery_record_for_id': job_id,
        'record_id': _id,
        'timing': timing
    }
    db = get_data_from_encr_db()
    prev = db.find_one({'recovery_record_for_id': job_id})
    if(prev):
        rec['timing'] = prev['timing']
        db.delete_one({'recovery_record_for_id': job_id})
        db.insert_one(rec)
    else:
        db.insert_one(rec)


def get_recovery_data(job_id: str = None) -> Any:
    '''
        While updating in sync mode, we try to find if the system had stopped anywhere in between last time. this helps us in updating all inserted records which were updated later but got missed.
    '''
    db = get_data_from_encr_db()
    prev = db.find_one({'recovery_record_for_id': job_id})
    if(prev):
        return prev
    else:
        return None

def delete_recovery_data(job_id: str = None) -> None:
    db = get_data_from_encr_db()
    db.delete_many({'recovery_record_for_id': job_id})



#############
#############
#############
#############
#############
#############
#############

dashboard = settings['dashboard_store']

def get_data_from_dashboard_db():
    '''
        Function to get connection to the dashboard database/collection
    '''
    try:
        certificate = 'config/rds-combined-ca-bundle.pem'
        client_dashboard = MongoClient(dashboard['url'], tlsCAFile=certificate)
        db_dashboard = client_dashboard[dashboard['db_name']]
        collection_dashboard = db_dashboard[dashboard['collection_name']]
        return collection_dashboard
    except Exception as e:
        raise ConnectionError("Unable to connect to Dashboard DB.") from e


def get_job_records(job_id: str = None) -> int:
    db = get_data_from_dashboard_db()
    prev = db.find({'job_id': job_id})
    prev = list(prev)
    if(len(prev) > 0):
        prev = list(prev)
        prev = prev[-1]
        return prev['total_records']
    else:
        return None


def get_job_mb(job_id: str = None) -> int:
    db = get_data_from_dashboard_db()
    prev = db.find({'job_id': job_id})
    prev = list(prev)
    if(len(prev) > 0):
        prev = list(prev)
        prev = prev[-1]
        return prev['total_megabytes']
    else:
        return None


def save_job_data(data: Dict = {}) -> None:
    db = get_data_from_dashboard_db()
    '''
        data = {
            job_id: str (unique),
            table_name: str,

            insertions: int,
            updations: int,
            total_records: int,
            start_time: timestamp,
            total_time: int (seconds),
            curr_megabytes_processed: int (mb),
            total_megabytes: int (mb),
            status: bool
        }
    '''
    db.insert_one(data)