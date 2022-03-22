from datetime import datetime
import logging
from typing import Any

from config.migration_mapping import settings
from pymongo import MongoClient
import certifi
from helper.exceptions import ConnectionError



from typing import NewType, Any
datetype = NewType("datetype", datetime)

encryption_store = settings['encryption_store']

def get_data_from_encr_db():
    try:
        client_encr = MongoClient(encryption_store['url'], tlsCAFile=certifi.where())
        db_encr = client_encr[encryption_store['db_name']]
        collection_encr = db_encr[encryption_store['collection_name']]
        return collection_encr
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
    db = get_data_from_encr_db()
    db.insert_one(rec)


class Log_manager:
    def __init__(self) -> None:
        # logger.add(sys.stdout, colorize=True, format="<green>{time:HH:mm:ss}</green> | {level} | <level>{message}</level>")
        logging.basicConfig(
            format = '%(asctime)s %(levelname)-8s %(message)s',
            level = logging.INFO,
            datefmt = '%Y-%m-%d %H:%M:%S'
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