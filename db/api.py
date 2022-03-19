from clevertap import ClevertapManager
from helper.logging import logger
from typing import Dict, Any
import pytz

IST_tz = pytz.timezone('Asia/Kolkata')

class APIMigrate:
    def __init__(self, db: Dict[str, Any] = None, curr_mapping: Dict[str, Any] = None, tz_str: str = 'Asia/Kolkata') -> None:
        self.db = db
        self.curr_mapping = curr_mapping
        self.tz_info = pytz.timezone(tz_str)
        self.client = None
        
    def save_data_to_destination(self, processed_data, partition):
        pass

    def process(self) -> None:
        if (self.db['source']['source_client'] and self.db['source']['source_client']=='clevertap'):
            self.client = ClevertapManager(self.curr_mapping['project_name'])
            event_names = self.client.set_and_get_event_names(self.curr_mapping['event_namess'])
            for event_name in event_names:
                try:
                    processed_data = self.client.get_processed_data(event_name, self.curr_mapping)
                    self.save_data_to_destination(processed_data=processed_data)
                except:
                    logger.err(self.curr_mapping['api_unique_id'] + " - "+ event_name + ": " + "caught some error while migrating event data.")

