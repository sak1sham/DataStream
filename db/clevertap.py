from config.clevertap import *
import requests
import json
from slugify import slugify
from dst.main import DMS_exporter
from helper.util import get_yyyymmdd_from_date, transformTs, validate_or_convert, typecast_df_to_schema
import pytz
import pandas as pd
import os
from typing import Dict, Any, List
from helper.logging import logger
import traceback

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
        return cx_app_event_names if project_name=='cx_app' else cl_app_event_names if project_name=='cl_app' else None

    def get_event_cursor(self, event_name: str, from_date: str, to_date: str):
        params = {
            "batch_size": os.getenv('CLEVERTAP_BATCHH_SIZE'), 
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
            raise Exception("Get cursor API did not return success status")

    def get_records_for_cursor(self, cursor):
        data = self.make_request("events.json?cursor={}".format(cursor), data="").json()
        return data

    def make_request(self, endpoint: str, data: Dict[str, Any]=None, params: Dict[str, Any]=None):
        res = requests.post(self.CLEVERTAP_API_BASE_URL + endpoint, data=json.dumps(data), params=params, headers=self.CLEVERTAP_API_HEADERS)
        if res.status_code != 200:
            raise Exception("Request to {2} returned an error {0}:\n{1}".format(res.status_code, res.text, self.CLEVERTAP_API_BASE_URL + endpoint))    
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
            raise Exception("invalid event names")
        return self.event_names

    def transform_api_data(self, records: List[Any], event_name: str, curr_mapping: Dict[str, Any]):
        tf_records = []
        for record in records:
            tf_record = {
                "event_name": event_name,
                "ct_ts": record.get("ts"),
                "timestamp": transformTs(record.get("ts", "")),
                "name": record["profile"].get("name", ""),
                "phone": record["profile"].get("phone", ""),
                "cx_city": record["profile"].get("profileData", {}).get("cx_city", ""),
                "city": record["profile"].get("profileData", {}).get("city", ""),
                "user_id": record["profile"].get("profileData", {}).get("user_id"),
                "whatsapp_opted_in":record["profile"].get("profileData", {}).get("whatsapp_opted_in", ""),

                "leader_id": record["profile"].get("profileData", {}).get("leaderid", ""),
                "leader_name": record["profile"].get("profileData", {}).get("leadername", ""),
                "leader_user_id": record["profile"].get("profileData", {}).get("leaderuserid", 0),
                "leader_lat": record["profile"].get("profileData", {}).get("leaderlat", 0.0),
                "leader_lng": record["profile"].get("profileData", {}).get("leaderlng", 0.0),
                "catalogue_name": record["profile"].get("profileData", {}).get("catalogue_name", ""),
                "platform": record["profile"].get("platform", ""),

                "ct_object_id": record["profile"].get("objectId", ""),
                "ct_session_id": record.get("event_props", {}).get("CT Session Id", ""),
                "screen_name": record.get("event_props", {}).get("screen_name", ""),

                "os_version": record["profile"].get("os_version", ""),
                "app_version": record["profile"].get("app_version", ""),
                "make": record["profile"].get("make", ""),
                "model": record["profile"].get("model", ""),
                "cplabel": record["profile"].get("profileData", {}).get("cplabel", ""),

                "tags": record["profile"].get("profileData", {}).get("tags", ""),
                "event_props": record.get("event_props", "")
            }
            tf_record = validate_or_convert(tf_record, curr_mapping['fields'], self.tz_info)
            tf_records.append(tf_record)
        return tf_records

    def get_processed_data(self, event_name: str, curr_mapping: Dict[str, Any]):
        bookmark_key = curr_mapping.get('bookmark_key', '')
        if not bookmark_key:
            raise Exception("please provide bookmark key")
        sync_date = get_yyyymmdd_from_date(days=bookmark_key)
        logger.inform("Started CX app event {0} sync for date: {1}".format(event_name, str(sync_date)))
        start_cursor = self.get_event_cursor(event_name, sync_date, sync_date)
        cursor_data = self.get_records_for_cursor(start_cursor)
        transformed_records = []
        total_records = 0
        if cursor_data["status"] == "success":
            if "records" in cursor_data:
                total_records += len(cursor_data['records'])
                transformed_records += self.transform_api_data(cursor_data['records'], event_name, curr_mapping)
                while "next_cursor" in cursor_data:
                    cursor_data = self.get_records_for_cursor(cursor_data["next_cursor"])
                    if "records" in cursor_data:
                        total_records += len(cursor_data['records'])
                        transformed_records += self.transform_api_data(cursor_data['records'], event_name, curr_mapping)
        logger.inform("Total Clevertap events: " + str(total_records))
        logger.inform("Tatal Clevertap events after transformation: " + str(len(transformed_records)))
        return {
            'name': curr_mapping['api_name'],
            'df_insert': typecast_df_to_schema(pd.DataFrame(transformed_records), curr_mapping['fields']),
            "lob_fields_length": curr_mapping['lob_fields']
        }
    
    def cleaned_processed_data(self, event_name: str, curr_mapping: Dict[str, Any], dst_saver: DMS_exporter):
        bookmark_key = curr_mapping.get('bookmark_key', '')
        if not bookmark_key:
            raise Exception("please provide bookmark key")
        sync_date = get_yyyymmdd_from_date(days=bookmark_key)
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
                logger.err(traceback.format_exc())
                logger.err("Error in deleting existing event - {0} for date {1} in redshift. Exception: {2}".format(event_name, sync_date, str(e)))


