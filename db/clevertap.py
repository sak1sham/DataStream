from config.clevertap import *
import requests
import json

class EventsAPIManager:
    CLEVERTAP_API_BASE_URL = "https://in1.api.clevertap.com/1/"

    def __init__(self, project_name):
        self.CLEVERTAP_API_HEADERS = {'X-CleverTap-Account-Id': account_id, 'X-CleverTap-Passcode': passcode, 'Content-Type': 'application/json'}

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


class ClevertapManager:
    def __init__(self, project_name) -> None:
        self.project_name = project_name
    
    def get_event_names(self) -> None:
        return (self.project_name + '_event_names').lower()



