from db.clevertap import ClevertapManager
from helper.logger import logger
from typing import Dict, Any
import pytz
from dst.main import DMS_exporter
import traceback
from helper.util import convert_to_datetime
from db.encr_db import get_last_run_cron_job
import datetime

IST_tz = pytz.timezone('Asia/Kolkata')

class APIMigrate:
    def __init__(self, db: Dict[str, Any] = None, curr_mapping: Dict[str, Any] = None, tz_str: str = 'Asia/Kolkata') -> None:
        self.db = db
        self.curr_mapping = curr_mapping
        self.tz_info = pytz.timezone(tz_str)

    def save_data_to_destination(self, processed_data: Dict[str, Any]):
        if(not processed_data):
            return
        else:
            self.saver.save(processed_data = processed_data)

    def presetup(self) -> None:
        if('fields' not in self.curr_mapping.keys()):
            self.curr_mapping['fields'] = {}

        self.saver = DMS_exporter(db = self.db, uid = self.curr_mapping['unique_id'])

    def process(self) -> None:
        self.presetup()
        if (self.db['source']['db_name']=='clevertap'):
            self.client = ClevertapManager(self.curr_mapping['project_name'])
            event_names = self.client.set_and_get_event_names(self.curr_mapping['event_names'])
            for event_name in event_names:
                try:
                    processed_data = self.client.get_processed_data(event_name, self.curr_mapping)
                    self.client.cleaned_processed_data(event_name, self.curr_mapping, self.saver)
                    self.save_data_to_destination(processed_data=processed_data)
                except:
                    logger.err(traceback.format_exc())
                    logger.err(self.curr_mapping['unique_id'] + " - "+ event_name + ": " + "caught some error while migrating event data.")

        self.saver.close()