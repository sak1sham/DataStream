from db.clevertap import ClevertapManager
from db.facebook import FacebookManager
from helper.logger import logger
from typing import Dict, Any, List
import pytz
from dst.main import DMS_exporter
from helper.util import typecast_df_to_schema, get_date_from_days
import pandas as pd
from helper.exceptions import APIRequestError, MissingData, IncorrectMapping
from notifications.slack_notify import send_message
from config.settings import settings
from datetime import timedelta, datetime

IST_tz = pytz.timezone('Asia/Kolkata')
slack_token = settings['slack_notif']['slack_token']


class APIMigrate:
    def __init__(self, db: Dict[str, Any] = None, curr_mapping: Dict[str, Any] = None, tz_str: str = 'Asia/Kolkata') -> None:
        self.db = db
        self.curr_mapping = curr_mapping
        self.tz_info = pytz.timezone(tz_str)

    def inform(self, message: str = None) -> None:
        logger.inform(s=f"{self.curr_mapping['unique_id']}: {message}")

    def warn(self, message: str = None) -> None:
        logger.warn(s=f"{self.curr_mapping['unique_id']}: {message}")

    def err(self, error: Any = None) -> None:
        logger.err(s=f"{self.curr_mapping['unique_id']}: {error}")

    def save_data_to_destination(self, processed_data: Dict[str, Any], primary_keys: List[str] = None):
        if(not processed_data):
            return
        else:
            self.saver.save(processed_data=processed_data, primary_keys=primary_keys)

    def presetup(self) -> None:
        if('fields' not in self.curr_mapping.keys()):
            self.curr_mapping['fields'] = {}
        self.saver = DMS_exporter(db=self.db, uid=self.curr_mapping['unique_id'])
        self.channel = self.curr_mapping['slack_channel'] if 'slack_channel' in self.curr_mapping and self.curr_mapping['slack_channel'] else settings['slack_notif']['channel']

    def process_clevertap_events(self, event_names: List[Any] = [], sync_date: datetime = None, max_attempts: int = 3):
        if not event_names:
            return
        if max_attempts <= 0:
            msg = 'Unable to process following events: \n'
            for event_name in event_names:
                msg += event_name + '\n'
            msg += f"for *{self.curr_mapping['project_name']}*."
            send_message(msg=msg, channel=self.channel, slack_token=slack_token)
            return
        if max_attempts < 3:
            msg = 'Error while processing following events: \n'
            for event_name in event_names:
                msg += event_name + '\n'
            msg += f"for *{self.curr_mapping['project_name']}*. Will remigrate these events automatically"
            send_message(msg=msg, channel=self.channel, slack_token=slack_token)

        api_failed_events = []
        other_failed_events = []
        for event_name in event_names:
            total_fetch_events = 0
            transformed_total_events = 0
            try:
                self.client.cleaned_processed_data(event_name, self.curr_mapping, self.saver, sync_date)
                have_more_data = True
                event_cursor = None
                while have_more_data:
                    processed_data = self.client.get_processed_data(event_name, self.curr_mapping, sync_date, event_cursor)
                    if processed_data and processed_data['total_records'] > 0:
                        processed_data_df = typecast_df_to_schema(pd.DataFrame(processed_data['records']), self.curr_mapping['fields'])
                        self.save_data_to_destination(processed_data={
                            'name': self.curr_mapping['api_name'],
                            'df_insert': processed_data_df,
                            'lob_fields_length': self.curr_mapping['lob_fields'],
                            'filename': event_name,
                            'dtypes': self.curr_mapping['fields']
                        })
                        event_cursor = processed_data['event_cursor']
                        total_fetch_events += int(processed_data['total_records'])
                        transformed_total_events += int(processed_data_df.shape[0])
                    have_more_data = True if processed_data and processed_data['event_cursor'] else False
            except APIRequestError as e:
                msg = f"Error while fetching data for event: *{event_name}* for app *{self.curr_mapping['project_name']}* from source on *{str(sync_date)}*."
                send_message(msg=msg, channel=self.channel, slack_token=slack_token)
                msg += f'``` Exception: {str(e)}```'
                self.err(msg)
                api_failed_events.append(event_name)
            except Exception as e:
                msg = f"Something went wrong! Could not process event *{event_name}* for project *{self.curr_mapping['project_name']}*.``` Exception: {str(e)}```"
                send_message(msg=msg, channel=self.channel, slack_token=slack_token)
                self.err(msg)
                other_failed_events.append(event_name)
        if len(api_failed_events) > 0:
            self.process_clevertap_events(api_failed_events, sync_date, max_attempts-1)
        if len(other_failed_events) > 0:
            self.process_clevertap_events(other_failed_events, sync_date, max_attempts-2)

    def process_facebook_events(self, start_date: datetime = None, max_attempts: int = 3):
        if not start_date:
            return
        try:
            processing_data = {
                'name': self.curr_mapping['api_name'],
                'filename': self.curr_mapping["api_name"],
                'dtypes': self.curr_mapping["fields"]
            }
            if "insights" in self.curr_mapping["api_name"]:
                if self.db['destination']['destination_type'] == "redshift":
                    self.client.cleaned_processed_data(self.curr_mapping, self.saver, start_date)
                processed_df = self.client.get_ad_insights_from_api(
                    start_date, self.curr_mapping)
                processing_data["df_insert"] = processed_df
            elif "campaigns" in self.curr_mapping["api_name"]:
                processed_df = self.client.get_ad_campaigns_from_api(start_date, self.curr_mapping)
                processing_data["df_update"] = processed_df

            self.save_data_to_destination(processed_data=processing_data, primary_keys=self.curr_mapping["primary_keys"])
        except APIRequestError as e:
            msg = f"Error while fetching data for table: *{self.curr_mapping['api_name']}* for app *{self.curr_mapping['project_name']}* from source on *{str(start_date)}*."
            send_message(msg=msg, channel=self.channel,
                         slack_token=slack_token)
            msg += f"``` Exception: {str(e)}```"
            self.err(msg)
        except Exception as e:
            msg = f"Something went wrong! Could not process table *{self.curr_mapping['api_name']}* for project *{self.curr_mapping['project_name']}*.``` Exception: {str(e)}```"
            send_message(msg=msg, channel=self.channel,
                         slack_token=slack_token)
            self.err(msg)

    def presetup_clevertap_process(self) -> None:
        start_day = self.curr_mapping.get('start_day', -1)
        end_day = self.curr_mapping.get('end_day', -1)
        if not start_day or not end_day:
            raise MissingData("please provide start_day and end_day in mapping")
        start_date = get_date_from_days(start_day, self.tz_info)
        end_date = get_date_from_days(end_day, self.tz_info)
        event_names = self.client.set_and_get_event_names(self.curr_mapping['event_names'])
        while start_date <= end_date:
            msg = f"Migration started for *{self.curr_mapping['project_name']}* events from database *{self.db['source']['db_name']}* to *{self.db['destination']['destination_type']}* for date: *{str(start_date)}*"
            send_message(msg, self.channel, slack_token)
            self.process_clevertap_events(event_names, start_date, 3)
            start_date += timedelta(1)

    def presetup_facebook_process(self) -> None:
        start_day = self.curr_mapping.get('start_day', -1)
        end_day = self.curr_mapping.get('end_day', -1)
        if not start_day or not end_day:
            raise MissingData(
                "please provide start_day and end_day in mapping")
        start_date = get_date_from_days(start_day, self.tz_info)
        end_date = get_date_from_days(end_day, self.tz_info)
        while start_date <= end_date:
            table = self.curr_mapping['project_name'] + '_' + self.curr_mapping["api_name"]
            msg = f"Migration started for *{table}* from database *{self.db['source']['db_name']}* to *{self.db['destination']['destination_type']}* for date: *{str(start_date)}*"
            send_message(msg, self.channel, slack_token)
            self.process_facebook_events(start_date)
            start_date += timedelta(1)

    def process(self) -> None:
        self.presetup()
        if (self.db['source']['db_name'] == 'clevertap'):
            self.client = ClevertapManager(self.curr_mapping['project_name'])
            self.presetup_clevertap_process()
        elif (self.db["source"]["db_name"] == "facebook_ads"):
            self.client = FacebookManager(
                self.curr_mapping['project_name'], self.db["source"])
            self.presetup_facebook_process()
        self.saver.close()
