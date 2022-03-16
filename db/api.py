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
    
    def inform(self, message: str = None) -> None:
        logger.inform(self.curr_mapping['unique_id'] + ": " + message)

    def warn(self, message: str = None) -> None:
        logger.warn(self.curr_mapping['unique_id'] + ": " + message)

    def err(self, error: Any = None) -> None:
        logger.err(error)

    
    def process(self) -> None:
        if (self.db['source']['source_client'] and self.db['source']['source_client']=='clevertap'):
            self.client = ClevertapManager(self.curr_mapping['project_name'])
            event_names = []
            if (isinstance(self.curr_mapping['event_names'], str)) and self.curr_mapping['event_names']=='*':
                event_names = self.client.get_event_names()
            if (isinstance(self.curr_mapping['event_names'], list)):
                event_names = self.curr_mapping['event_names']
            else:
                raise Exception("invalid event names")
            for event in event_names:
                pass
