from db.clevertap import ClevertapManager
from helper.logger import logger
from typing import Dict, Any
import pytz
from dst.main import DMS_exporter
import traceback
from helper.util import typecast_df_to_schema
import time
import pandas as pd

IST_tz = pytz.timezone('Asia/Kolkata')

class APIMigrate:
    def __init__(self, db: Dict[str, Any] = None, curr_mapping: Dict[str, Any] = None, tz_str: str = 'Asia/Kolkata') -> None:
        self.db = db
        self.curr_mapping = curr_mapping
        self.tz_info = pytz.timezone(tz_str)
    
    def inform(self, message: str = None, save: bool = False) -> None:
        logger.inform(job_id = self.curr_mapping['unique_id'], s=(self.curr_mapping['unique_id'] + ": " + message), save=save)

    def warn(self, message: str = None) -> None:
        logger.warn(job_id = self.curr_mapping['unique_id'], s=(self.curr_mapping['unique_id'] + ": " + message))

    def err(self, error: Any = None) -> None:
        logger.err(job_id= self.curr_mapping['unique_id'], s=error)

    def save_data_to_destination(self, processed_data: Dict[str, Any]):
        if(not processed_data):
            return
        else:
            self.saver.save(processed_data = processed_data)

    def presetup(self) -> None:
        if('fields' not in self.curr_mapping.keys()):
            self.curr_mapping['fields'] = {}
        self.saver = DMS_exporter(db = self.db, uid = self.curr_mapping['unique_id'])
    
    def process_clevertap_events(self):
        self.client = ClevertapManager(self.curr_mapping['project_name'])
        event_names = self.client.set_and_get_event_names(self.curr_mapping['event_names'])
        for event_name in event_names:
            self.client.cleaned_processed_data(event_name, self.curr_mapping, self.saver)
            have_more_data = True
            event_cursor = None    
            while have_more_data:
                processed_data = self.client.get_processed_data(event_name, self.curr_mapping, event_cursor)
                if processed_data:
                    processed_data_df = typecast_df_to_schema(pd.DataFrame(processed_data['records']), self.curr_mapping['fields'])
                    self.inform('migrating {0} event for {1}'.format(event_name, self.curr_mapping['project_name']))
                    self.save_data_to_destination(processed_data={
                        'name': self.curr_mapping['api_name'],
                        'df_insert': processed_data_df,
                        'lob_fields_length': self.curr_mapping['lob_fields']
                    })
                    event_cursor = processed_data['event_cursor']
                    have_more_data = True if processed_data['event_cursor'] else False
                time.sleep(1)

    def process(self) -> None:
        self.presetup()
        if (self.db['source']['db_name']=='clevertap'):
            self.process_clevertap_events()
        self.saver.close()