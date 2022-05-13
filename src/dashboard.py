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
    

    def delete_metadata_from_mongodb(self, unique_id: str = None) -> None:
        '''
            Delete all records from mongodb temporary data
        '''
        self.metadata_col.delete_many({'last_migrated_record_for_id': unique_id})
        self.metadata_col.delete_many({'last_run_cron_job_for_id': unique_id})
        self.metadata_col.delete_many({'recovery_record_for_id': unique_id})
        self.metadata_col.delete_many({'table': unique_id})
        self.metadata_col.delete_many({'collection': unique_id})


if __name__ == "__main__":
    jobs_path = os.path.abspath(os.getcwd()) + '/config/jobs/'
    metadata = Metadata()
    job_list = []
    for file in os.listdir(jobs_path):
        if file.endswith(".py"):
            job_list.append(file[:-3])
    job_list.sort()
    st.write('# Data Migration Service')
    col1, col2, col3 = st.columns(3)
    id_suffix = ''
    suffix_include = True
    id_prefix = ''
    prefix_include = True
    id_substr = ''
    substr_include = True

    with col1:
        id_suffix = st.text_input('Suffix', '')
        suffix_include = st.checkbox('Included', value=1, key='suffix_include')
    with col2:
        id_prefix = st.text_input('Prefix', '')
        prefix_include = st.checkbox('Included', value=1, key='prefix_include')
    with col3:
        id_substr = st.text_area('Substrings (separate lines)', placeholder='substring-1\nsubstring-2')
        substr_include = st.checkbox('Included', value=1, key='substr_include')
    
    if(suffix_include):
        job_list = [x for x in job_list if x.endswith(id_suffix)]
    else:
        job_list = [x for x in job_list if not x.endswith(id_suffix)]
    
    if(prefix_include):
        job_list = [x for x in job_list if x.startswith(id_prefix)]
    else:
        job_list = [x for x in job_list if not x.startswith(id_prefix)]
    
    id_substr = id_substr.strip().split()
    if(substr_include):
        job_list = [x for x in job_list if any(y in x for y in id_substr)]
    else:
        job_list = [x for x in job_list if not any(y in x for y in id_substr)]
    
    #job_list = [x for x in job_list if x.endswith(id_suffix) and x.startswith(id_prefix) and not any(y in x for y in substr_include)]
    job_id_selected = st.selectbox(
        "Please select a job id from the following:",
        tuple(job_list)
    )
    st.write('You selected:', job_id_selected)
    if(job_id_selected):
        job_data = metadata.get_metadata(job_id=str(job_id_selected))
        datetime_format = '%A %d %B, %Y @ %I:%M:%S %p IST'
        for key, val in job_data.items():
            unique_id = job_id_selected + "_DMS_" + key
            last_run_cron_job = val[0]
            last_migrated_record = val[1]
            recovery_data = val[2]
            st.write('##', key)
            if(last_run_cron_job):
                st.write("Job started at " + last_run_cron_job['timing'].strftime(datetime_format))
            if(last_migrated_record):
                st.write("Last inserted primary key: ", last_migrated_record['record_id'])
                st.write("Last insertion done on " + last_migrated_record['timing'].strftime(datetime_format))
            if(recovery_data):
                st.write("Last job stopped unexpectedly at ", recovery_data['timing'].strftime(datetime_format))
            if(not (last_migrated_record or last_run_cron_job or recovery_data)):
                st.write("No data to show for this job.")
            else:
                unique_confirmation_key = "Delete_confirmation_for" + key
                unique_button_key = "Delete_button_for" + key
                deletion_confirmation = st.text_input('Type \'Permanently Delete Metadata\' in the following box', key=unique_confirmation_key)
                if st.button('Delete all Metadata for this mapping', key=unique_button_key) and deletion_confirmation == 'Permanently Delete Metadata':
                    metadata.delete_metadata_from_mongodb(job_id_selected + "_DMS_" + key)
                    st.write('Deleted metadata for ', unique_id)
            show_metadata = st.checkbox(label="Show metadata", value=False, key=key)
            if(show_metadata):
                st.write(val)