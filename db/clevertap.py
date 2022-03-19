from config.clevertap import *
import requests
import json
from slugify import slugify

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
    def __init__(self, project_name) -> None:
        self.project_name = project_name
        self.event_names = []
        supper().__init__(project_name)
    
    def set_and_get_event_names(self, event_names) -> None:
        if (isinstance(event_names, str)) and event_names=='*':
            self.event_names = (self.project_name + '_event_names').lower()
        if (isinstance(event_names, list)):
            self.event_names = event_names
        else:
            raise Exception("invalid event names")
        return self.event_names

    def api_to_dataframe(self, records, event_name, from_date):
        file_name = event_name + '-' + str(from_date) + '-' + str(batch_count) + '.csv'
        year = str(from_date)[0:4]
        month = str(from_date)[4:6]
        day = str(from_date)[6:8]
        event_slug = slugify(event_name)

        with open(file_name, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            for record in records:
                leaderlat=0.0
                leaderlng=0.0
                leaderuserid = 0
                try:
                    leaderlat = float(record["profile"].get("profileData", {}).get("leaderlat", 0.0))
                except:
                    pass
                try:        
                    leaderlng = float(record["profile"].get("profileData", {}).get("leaderlng", 0.0))
                except:
                    pass
                try:        
                    leaderuserid = int(record["profile"].get("profileData", {}).get("leaderuserid", 0))
                except:
                    pass

                try:
                    csv_writer.writerow([
                        event_name,
                        record.get("ts", ""),
                        transformTs(record.get("ts", "")),
                        record["profile"].get("name", ""),
                        record["profile"].get("phone", ""),
                        record["profile"].get("profileData", {}).get("cx_city", ""),
                        record["profile"].get("profileData", {}).get("city", ""),
                        int(record["profile"].get("profileData", {}).get("user_id")),
                        record["profile"].get("profileData", {}).get("whatsapp_opted_in", ""),

                        record["profile"].get("profileData", {}).get("leaderid", ""),
                        record["profile"].get("profileData", {}).get("leadername", ""),
                        leaderuserid,
                        leaderlat,
                        leaderlng,
                        record["profile"].get("profileData", {}).get("catalogue_name", ""),
                        record["profile"].get("platform", ""),

                        record["profile"].get("objectId", ""),
                        record.get("event_props", {}).get("CT Session Id", ""),
                        record.get("event_props", {}).get("screen_name", ""),

                        record["profile"].get("os_version", ""),
                        record["profile"].get("app_version", ""),
                        record["profile"].get("make", ""),
                        record["profile"].get("model", ""),
                        record["profile"].get("profileData", {}).get("cplabel", ""),

                        json.dumps(record["profile"].get("profileData", {}).get("tags", "")),
                        json.dumps(record.get("event_props", ""))
                    ])
                except:
                    pass

    def get_processed_data(self, event_name, curr_mapping):
        records = []
        start_cursor = self.get_event_cursor(event_name, start_date, end_date)
        cursor_data = self.get_records_for_cursor(start_cursor)
        if cursor_data["status"] == "success":
            if "records" in cursor_data:
                records += cursor_data['records']
                while "next_cursor" in cursor_data:
                    cursor_data = self.get_records_for_cursor(cursor_data["next_cursor"])
                    if "records" in cursor_data:
                        records += cursor_data['records']
        return records



