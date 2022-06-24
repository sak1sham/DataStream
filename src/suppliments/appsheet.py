from helper.util import convert_to_dtype
from dst.main import DMS_exporter

import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    'source': {
        'source_type': 'Appsheet',
    },
    "destination": {
        "db_name": "cmdb",
        "schema": "public",
        "password": os.getenv('DB_PASSWORD'),
        "url": "15.206.171.84",
        "username": os.getenv('DB_USERNAME'),
        "destination_type": "pgsql",
    },
    'tables': [            
        {
            'table_name': 'appsheet_selections_pre_interview_respones',
            'col_rename': {'timestamp':'timestamp_'},
            'dtypes': {
                'pkey': 'int',
                'timestamp_': 'datetime',
                'cl_mobile_number': 'int'
            }
        },
    ]
}

dtypes = mapping['tables'][0]['dtypes']
sheet_url = "https://docs.google.com/spreadsheets/d/1Jy1PEy8azw12NxwbHo660vmtGJuapuSS5og_HSMZW8Y/edit#gid=1501248197"
url_1 = sheet_url.replace('/edit#gid=', '/export?format=csv&gid=')

df = pd.read_csv(url_1)
df['pkey'] = df.index

cols = list(df.columns)
for i in range(len(cols)):
    cols[i] = cols[i].replace("/", "_").replace("-", "_").replace(".", "_").replace("'", "_").replace("?","_").replace("%","_").lower()
df.columns = cols

if('col_rename' in mapping['tables'][0].keys()):
    df.rename(columns = mapping['tables'][0]['col_rename'], inplace = True)


df = convert_to_dtype(df, dtypes)

saver = DMS_exporter(db = mapping, uid = "appsheet_selections_pre_interview_respones")

processed_data = {
    'df_insert': df,
    'dtypes': dtypes,
    'name': mapping['tables'][0]['table_name'],
    'col_rename': {} if 'col_rename' not in mapping['tables'][0].keys() else mapping['tables'][0]['col_rename']
}

saver.save(processed_data=processed_data, primary_keys=['pkey'])