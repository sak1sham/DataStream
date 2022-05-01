import streamlit as st
from dotenv import load_dotenv
load_dotenv()
from pymongo import MongoClient
from typing import Any, Dict
import pytz
import os

from config.migration_mapping import get_mapping
from config.settings import settings
encryption_store = settings['encryption_store']

group_key = {
    'sql': 'tables',
    'mongo': 'collections',
    'api': 'apis',
    's3': 'tables',
    'kafka': 'topics'
}


class Metadata():
    def __init__(self) -> None:
        certificate = 'config/rds-combined-ca-bundle.pem'
        client_encr = MongoClient(encryption_store['url'], tlsCAFile=certificate)
        db_encr = client_encr[encryption_store['db_name']]
        self.metadata_col = db_encr[encryption_store['collection_name']]


    def process_timing(self, res: Any = None) -> Any:
        if(res):
            res['timing'] = (pytz.utc.localize(res['timing'])).astimezone(pytz.timezone('Asia/Kolkata'))
        else:
            res = None
        return res


    def get_metadata(self, job_id: str = None) -> Dict[str, Any]:
        mapping = get_mapping(job_id)
        source_type = mapping['source']['source_type']
        ret_metadata = {}
        for curr_map in mapping[group_key[source_type]]:
            name_key = group_key[source_type][:-1] + "_name"
            name = curr_map[name_key]
            unique_id = job_id + "_DMS_" + name
            last_run_cron_time = self.metadata_col.find_one({'last_run_cron_job_for_id': unique_id})
            self.process_timing(last_run_cron_time)

            last_migrated_record = self.metadata_col.find_one({'last_migrated_record_for_id': unique_id})
            self.process_timing(last_migrated_record)
            
            recovery_data = self.metadata_col.find_one({'recovery_record_for_id': unique_id})
            self.process_timing(recovery_data)
            
            ret_metadata[name] = [last_run_cron_time, last_migrated_record, recovery_data]
        
        return ret_metadata


if __name__ == "__main__":
    jobs_path = os.path.abspath(os.getcwd()) + '/config/jobs/'
    job_list = []
    for file in os.listdir(jobs_path):
        if file.endswith(".py"):
            job_list.append(file[:-3])
    job_list.sort()
    st.write('# Data Migration Service')
    option = st.selectbox(
        "Please select a job id from the following:",
        tuple(job_list)
    )
    st.write('You selected:', option)
    metadata = Metadata()
    job_data = metadata.get_metadata(job_id=str(option))
    datetime_format = '%A %d %B, %Y @ %I:%M:%S %p IST'
    for key, val in job_data.items():
        last_run_cron_job = val[0]
        last_migrated_record = val[1]
        recovery_data = val[2]
        st.write('##', key)
        if(last_migrated_record):
            st.write("Last inserted primary key: ", last_migrated_record['record_id'])
        if(last_run_cron_job):
            st.write("Job started at " + last_run_cron_job['timing'].strftime(datetime_format))
        if(last_migrated_record):
            st.write("Insertions completed at " + last_migrated_record['timing'].strftime(datetime_format))
        if(recovery_data):
            st.write("Last job stopped unexpectedly at ", recovery_data['timing'].strftime(datetime_format))
        if(not (last_migrated_record or last_run_cron_job or recovery_data)):
            st.write("This job never started.")
        show_metadata = st.checkbox(label="Show metadata", value=False, key=key)
        if(show_metadata):
            st.write(job_data)