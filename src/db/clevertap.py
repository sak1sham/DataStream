from config.clevertap import cx_app_event_names, cl_app_event_names, cx_web_event_names
import requests
import json
from dst.main import DMS_exporter
from helper.util import get_yyyymmdd_from_date, transformTs, validate_or_convert, extract_value_from_nested_obj
import pytz
import os
from typing import Dict, Any, List
from helper.logger import logger
import traceback
from helper.exceptions import MissingData, APIRequestError, IncorrectMapping
from datetime import datetime

class EventsAPIManager:
    CLEVERTAP_API_BASE_URL = os.getenv('CLEVERTAP_BASE_URL')

    def __init__(self, project_name: str):
        self.CLEVERTAP_API_HEADERS = {
            'X-CleverTap-Account-Id': os.getenv('CLEVERTAP_'+str(project_name).upper()+'_ACCOUNT_ID'), 
            'X-CleverTap-Passcode': os.getenv('CLEVERTAP_'+str(project_name).upper()+'_PASSCODE'), 
            'Content-Type': 'application/json'
        }

    @staticmethod
    def get_static_value(project_name: str):
        return cx_app_event_names if project_name=='cx_app' else cl_app_event_names if project_name=='cl_app' else cx_web_event_names if project_name=='cx_web' else None

    def get_event_cursor(self, event_name: str, from_date: str, to_date: str):
        params = {
            "batch_size": os.getenv('CLEVERTAP_BATCH_SIZE'), 
            "events": "false"
        }
        payload = {
            "event_name": event_name,
            "from": from_date,
            "to": to_date
        }
        data = self.make_request("events.json", data=payload, params=params).json()
        if data["status"] == "success":
            return data["cursor"]
        else:
            raise APIRequestError("Get cursor API did not return success status")

    def get_records_for_cursor(self, cursor):
        data = self.make_request("events.json?cursor={}".format(cursor), data="").json()
        return data

    def make_request(self, endpoint: str, data: Dict[str, Any]=None, params: Dict[str, Any]=None):
        res = requests.post(self.CLEVERTAP_API_BASE_URL + endpoint, data=json.dumps(data), params=params, headers=self.CLEVERTAP_API_HEADERS)
        if res.status_code != 200:
            raise APIRequestError("Request to {2} returned an error {0}:\n{1}".format(res.status_code, res.text, self.CLEVERTAP_API_BASE_URL + endpoint))    
        return res


class ClevertapManager(EventsAPIManager):
    def __init__(self, project_name: str, tz_str: str = 'Asia/Kolkata') -> None:
        self.project_name = project_name
        self.event_names = []
        self.tz_info = pytz.timezone(tz_str)
        super().__init__(self.project_name)    
    
    def set_and_get_event_names(self, event_names: Any):
        if (isinstance(event_names, str)) and event_names=='*':
            self.event_names = self.get_static_value(self.project_name)
        elif (isinstance(event_names, list)):
            self.event_names = event_names
        else:
            raise IncorrectMapping("invalid event names")
        return self.event_names

    def transform_api_data(self, records: List[Any], event_name: str, curr_mapping: Dict[str, Any]):
        tf_records = []
        for record in records:
            tf_record = {
                "event_name": event_name,
                "timestamp": transformTs(record.get("ts", "")),
            }
            for key, value in curr_mapping['api_to_field_mapping'].items():
                result  = extract_value_from_nested_obj(record, value)
                if not result:
                    if curr_mapping['fields'][key]=='str':
                        result = ''
                    if curr_mapping['fields'][key]=='int':
                        result = 0
                tf_record[key] = result
            try:
                tf_record = validate_or_convert(tf_record, curr_mapping['fields'], self.tz_info)
                tf_records.append(tf_record)
            except:
                logger.err(curr_mapping['unique_id'], curr_mapping['unique_id']+ ": Could not process record for event {0}. Record: ```{1}```".format(event_name, str(tf_record)))
        return tf_records

    def get_processed_data(self, event_name: str, curr_mapping: Dict[str, Any], sync_date: datetime, event_cursor: str = None):
        if not event_cursor:
            sync_date = get_yyyymmdd_from_date(sync_date)
            logger.inform(curr_mapping['unique_id'], curr_mapping['unique_id']+": started {2} event {0} sync for date: {1}".format(event_name, str(sync_date), self.project_name))
            event_cursor = self.get_event_cursor(event_name, sync_date, sync_date)
        cursor_data = self.get_records_for_cursor(event_cursor)
        total_records = 0
        records = []
        next_cursor = None
        if cursor_data["status"] == "success":
            if "records" in cursor_data:
                total_records = len(cursor_data['records'])
                records = self.transform_api_data(cursor_data['records'], event_name, curr_mapping)
                next_cursor = cursor_data['next_cursor'] if 'next_cursor' in cursor_data else None
        return {
            'records': records,
            'event_cursor': next_cursor,
            'total_records': total_records
        }
    
    def cleaned_processed_data(self, event_name: str, curr_mapping: Dict[str, Any], dst_saver: DMS_exporter, sync_date: datetime):
        sync_date = get_yyyymmdd_from_date(sync_date)
        year = str(sync_date)[0:4]
        month = str(sync_date)[4:6]
        day = str(sync_date)[6:8]

        if dst_saver.type=='redshift':
            try:
                cur = dst_saver.saver.conn.cursor()
                cur.execute("select 1 from pg_tables where schemaname = %s and tablename = %s", (dst_saver.saver.schema, curr_mapping['api_name']))
                result = cur.fetchone()
                if result:
                    delete_query = "delete from {0}.{1} where DATE(timestamp) = '{3}-{4}-{5}' and event_name='{2}'".format(
                        dst_saver.saver.schema,
                        curr_mapping['api_name'],
                        event_name,
                        year, 
                        month, 
                        day
                    )
                    cur.execute(delete_query)
                dst_saver.saver.conn.commit()
                cur.close()
            except Exception as e:
                logger.err(curr_mapping['unique_id'], traceback.format_exc())
                logger.err(curr_mapping['unique_id'], curr_mapping['unique_id']+ ": Error in deleting existing event - {0} for date {1} in redshift. Exception: {2}".format(event_name, sync_date, str(e)))


