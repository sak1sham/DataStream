from datetime import datetime
import logging
from typing import Any
import os

from config.settings import settings
from pymongo import MongoClient
from helper.exceptions import ConnectionError

from typing import NewType, Any
datetype = NewType("datetype", datetime)

logging_store = settings['logging_store']

def get_data_from_log_db():
    try:
        certificate = 'config/rds-combined-ca-bundle.pem'
        client_log = MongoClient(logging_store['url'], tlsCAFile=certificate)
        db_log = client_log[logging_store['db_name']]
        collection_log = db_log[logging_store['collection_name']]
        return collection_log
    except:
        raise ConnectionError("Unable to connect to Encryption DB.")

# function to write logs into mongodb
def write_logs(job_id: str = None, log: str = None, timing: datetype = datetime.utcnow(), type_of_log: int = 0) -> None:
    rec = {
        'log_for_job': job_id,
        'log': str(log),
        'timing': timing,
        'type_of_log': type_of_log
    }
    db = get_data_from_log_db()
    db.insert_one(rec)


class Log_manager:
    def __init__(self) -> None:
        p = os.path.abspath(os.getcwd())
        logs_folder = p + '/tmp'
        isExist = os.path.exists(logs_folder)
        if not isExist:
            os.makedirs(logs_folder)
            print("The new directory is created to save logs!")
        logging.basicConfig(
            format = '%(asctime)s %(levelname)-8s %(message)s',
            level = logging.INFO,
            datefmt = '%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler(logs_folder + '/debug.log', mode='w'),
                logging.StreamHandler()
            ]
        )
        logging.getLogger().setLevel(logging.INFO)


    def inform(self, job_id: str = None, s: str = None, save : bool = False) -> None:
        logging.info(s)
        if(save and 'save_logs' in settings.keys() and settings['save_logs']):
            write_logs(job_id, s, 0)
    def warn(self, job_id: str = None, s: str = None) -> None:
        logging.warning(s)
        if ('save_logs' in settings.keys() and settings['save_logs']):
            write_logs(job_id, s, 1)
    def err(self, job_id: str = None, s: Any = None) -> None:
        logging.error(s)
        if ('save_logs' in settings.keys() and settings['save_logs']):
            write_logs(job_id, s, 2)


logger = Log_manager()