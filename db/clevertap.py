from config.clevertap import *
import requests
import json
from slugify import slugify
from helper.util import get_yyyymmdd_from_date, transformTs, validate_or_convert, typecast_df_to_schema
import pytz
import pandas as pd

class EventsAPIManager:
    CLEVERTAP_API_BASE_URL = "https://in1.api.clevertap.com/1/"

    def __init__(self, project_name):
        self.CLEVERTAP_API_HEADERS = {
            'X-CleverTap-Account-Id': 'CLEVERTAP_'+ project_name.upper() + 'ACCOUNT_ID', 
            'X-CleverTap-Passcode': 'CLEVERTAP_'+ project_name.upper() + 'PASSCODE', 
            'Content-Type': 'application/json'
        }

    def get_event_cursor(self, event_name, from_date, to_date):
        params = {"batch_size": 10000, "events": "false"}
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

    def make_request(self, endpoint, data=None, params=None):
        res = requests.post(self.CLEVERTAP_API_BASE_URL + endpoint, data=json.dumps(data), params=params, headers=self.CLEVERTAP_API_HEADERS)
        if res.status_code != 200:
            raise Exception("Request to {2} returned an error {0}:\n{1}".format(res.status_code, res.text, self.CLEVERTAP_API_BASE_URL + endpoint))    
        return res


class ClevertapManager(EventsAPIManager):
    def __init__(self, project_name, tz_str: str = 'Asia/Kolkata') -> None:
        self.project_name = project_name
        self.event_names = []
        self.tz_info = pytz.timezone(tz_str)
        super().__init__(project_name)
    
    def set_and_get_event_names(self, event_names) -> None:
        if (isinstance(event_names, str)) and event_names=='*':
            self.event_names = (self.project_name + '_event_names').lower()
        if (isinstance(event_names, list)):
            self.event_names = event_names
        else:
            raise Exception("invalid event names")
        return self.event_names

    def transform_api_data(self, records, event_name, curr_mapping):
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

    def get_processed_data(self, event_name, curr_mapping):
        bookmark_key = curr_mapping.get('bookmark_key', '')
        if not bookmark_key:
            raise Exception("please provide bookmark key")
        sync_date = get_yyyymmdd_from_date(days=bookmark_key)
        start_cursor = self.get_event_cursor(event_name, sync_date, sync_date)
        cursor_data = self.get_records_for_cursor(start_cursor)
        transformed_records = []
        if cursor_data["status"] == "success":
            if "records" in cursor_data:
                transformed_records += self.transform_api_data(event_name, cursor_data['records'], curr_mapping)
                while "next_cursor" in cursor_data:
                    cursor_data = self.get_records_for_cursor(cursor_data["next_cursor"])
                    if "records" in cursor_data:
                        transformed_records += self.transform_api_data(event_name, cursor_data['records'], curr_mapping)
        return {
            'name': curr_mapping['api_name'],
            'df_insert': typecast_df_to_schema(pd.DataFrame(transformed_records), curr_mapping['fields'])
        }



