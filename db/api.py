from db.clevertap import ClevertapManager
from helper.logger import logger
from typing import Dict, Any, List
import pytz
from dst.main import DMS_exporter
import traceback
from helper.util import typecast_df_to_schema
import time
import pandas as pd
from helper.exceptions import APIRequestError
from notifications.slack_notify import send_message
from config.migration_mapping import settings

IST_tz = pytz.timezone('Asia/Kolkata')
slack_token = settings['slack_notif']['slack_token']

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
    
    def process_clevertap_events(self, event_names: List[Any]=[], max_attempts: int=3):
        if not event_names:
            return
        channel = self.curr_mapping['slack_channel'] if 'slack_channel' in self.curr_mapping and self.curr_mapping['slack_channel'] else settings['slack_notif']['channel']
        if max_attempts==0:
            msg = 'Unable to process following events: \n'
            for event_name in event_names:
                msg += event_name + '\n'
            msg += 'for {0}.'.format(self.curr_mapping['project_name'])
            send_message(msg = msg, channel = channel, slack_token = slack_token)
            return
        if max_attempts < 3:
            msg = 'Error while processing following events: \n'
            for event_name in event_names:
                msg += event_name + '\n'
            msg += 'for {0}. Will remigrate these events automatically'.format(self.curr_mapping['project_name'])
            send_message(msg = msg, channel = channel, slack_token = slack_token)

        failed_events = []
        for event_name in event_names:
            total_fetch_events = 0
            transformed_total_events = 0
            try:
                # self.client.cleaned_processed_data(event_name, self.curr_mapping, self.saver)
                have_more_data = True
                event_cursor = None    
                while have_more_data:
                    processed_data = self.client.get_processed_data(event_name, self.curr_mapping, event_cursor)
                    if processed_data:
                        processed_data_df = typecast_df_to_schema(pd.DataFrame(processed_data['records']), self.curr_mapping['fields'])
                        # self.save_data_to_destination(processed_data={
                        #     'name': self.curr_mapping['api_name'],
                        #     'df_insert': processed_data_df,
                        #     'lob_fields_length': self.curr_mapping['lob_fields']
                        # })
                        event_cursor = processed_data['event_cursor']
                        total_fetch_events += int(processed_data['total_records'])
                        transformed_total_events += int(processed_data_df.shape[0])
                        self.inform('new records: {0} event for {1} total records: {2}'.format(processed_data['total_records'], event_name, total_fetch_events))
                    have_more_data = True if processed_data and processed_data['event_cursor'] else False
                msg = '{0} out of {1} records saved for event: {2} for project: {3}.'.format(transformed_total_events, total_fetch_events, event_name, self.curr_mapping['project_name'])
                send_message(msg = msg, channel = channel, slack_token = slack_token)
            except APIRequestError as e:
                msg = 'Error while fetching data for event: {0} for app {1} from source. Exception: {2}'.format(event_name, self.curr_mapping['project_name'], str(e))
                self.err(msg)
                send_message(msg = msg, channel = channel, slack_token = slack_token)
                failed_events.append(event_name)
            except Exception as e:
                msg = "Something went wrong! Could not process event {0} for project {1}. Exception: {2}".format(event_name, self.curr_mapping['project_name'], str(e))
                send_message(msg = msg, channel = channel, slack_token = slack_token)
                self.err(msg)
        # self.process_clevertap_events(failed_events, max_attempts-1)

    def process(self) -> None:
        self.presetup()
        if (self.db['source']['db_name']=='clevertap'):
            self.client = ClevertapManager(self.curr_mapping['project_name'])
            event_names = self.client.set_and_get_event_names(self.curr_mapping['event_names'])
            self.process_clevertap_events( event_names, 3)
        self.saver.close()