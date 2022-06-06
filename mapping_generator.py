import pandas as pd
import json
import numpy as np
import re

df = pd.read_csv('abc.csv')
df = df.replace({np.nan:None})
print(df)

for index, row in df.iterrows():
    mapping = {}
    if(row['db'] == 'cmdb'):
        mapping['source'] = {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        }
    elif(row['db'] == 'wmsdb'):
        mapping['source'] = {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'wmsdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        }
    elif(row['db'] == 'crmdb'):
        mapping['source'] = {
            'source_type': 'sql',
            'url': 'crmdb.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'crmdb',
            'username': 'saksham_garg',
            'password': '3y5HMs^2qy%&Kma'
        }
    else:
        raise Exception(f"Not supported db type {row['db']}")
    
    mapping['tables'] = [
        {
            'table_name': row['table_name'],
            'cron': 'self-managed',
            'mode': row['mode'],
            'primary_key': row['pkey'],
            'primary_key_datatype': row['pkey_type'],
            'improper_bookmarks': False, 
        }
    ]
    if(row['mode'] == 'syncing'):
        mapping['tables'][0]['bookmark'] = row['bookmark']
        mapping['tables'][0]['buffer_updation_lag'] = {
            'hours': 2,
        }
        mapping['tables'][0]['grace_updation_lag'] = {
            'days': 1
        }
    
    if(row['size'] >= 7500000):
        mapping['tables'][0]['batch_size'] = 100000
    else:
        mapping['tables'][0]['batch_size'] = 10000
    
    mapping_pg = mapping.copy()
    if(row['partition']):
        mapping['tables'][0]['to_partition'] = True
        mapping['tables'][0]['partition_col'] = row['partition']
        mapping['tables'][0]['partition_col_format'] = 'datetime'

    mapping['destination'] = { 
        'destination_type': 's3', 
        'specifications': [
            {
                's3_bucket_name': 'database-migration-service-prod' 
            }
        ]
    }
    mapping_pg['destination'] = {
        'destination_type': 'pgsql',
        'specifications': [
            {
                'url': '3.108.43.163',
                'db_name': 'dms',
                'username': 'saksham_garg',
                'password': '3y5HMs^2qy%&Kma'
            }
        ]
    }

    file_name = f"{row['db']}_" if row['db'] in ['wmsdb', 'crmdb'] else ""
    file_name += row['table_name'].replace('.', '_').replace('-', '_')
    file_name_pg = file_name + "_to_analytics_pgsql.py"
    file_name += ".py"
    
    with open(f'src/config/jobs/{file_name}', 'w') as f:
        json.dump(mapping, f, sort_keys=True, indent=4)
    print(file_name)
    with open(f'src/config/jobs/{file_name_pg}', 'w') as f:
        json.dump(mapping_pg, f, sort_keys=True, indent=4)
    print(file_name_pg)


    dockerfile = file_name[:-3].replace('_', '-') + ".Dockerfile"
    dockerfile_pg = file_name_pg[:-3].replace('_', '-') + ".Dockerfile"

    with open(f'deployment/dockerfiles/{dockerfile}', 'w') as f:
        text = f'FROM python:3.8\n\nRUN pip3 install --upgrade pip==21.2.4\n\nRUN pip3 --no-cache-dir install --upgrade awscli &&\ \n\tpip3 install pip-tools\n\nCOPY requirements.in .\n\nRUN pip-compile &&\ \n\tpip-sync\n\nCOPY ./src /src\nWORKDIR "/src"\n\nCMD python main.py {file_name[:-3]}'
        f.write(text)

    with open(f'deployment/dockerfiles/{dockerfile_pg}', 'w') as f:
        text = f'FROM python:3.8\n\nRUN pip3 install --upgrade pip==21.2.4\n\nRUN pip3 --no-cache-dir install --upgrade awscli &&\ \n\tpip3 install pip-tools\n\nCOPY requirements.in .\n\nRUN pip-compile &&\ \n\tpip-sync\n\nCOPY ./src /src\nWORKDIR "/src"\n\nCMD python main.py {file_name_pg[:-3]}'
        f.write(text)

    with open(f".github/workflows/prod.kube.{file_name[:-3].replace('_', '-')}.yaml", 'w') as f:
        text = ''
        with open('.github/workflows/prod.kube.analytics-cl-funnel.yaml', 'r') as file:
            text = file.read()
        text = text.replace('analytics-cl-funnel', file_name[:-3].replace('_', '-'))
        f.write(text)

    with open(f".github/workflows/prod.kube.{file_name_pg[:-3].replace('_', '-')}.yaml", 'w') as f:
        text = ''
        with open('.github/workflows/prod.kube.analytics-cl-funnel-to-analytics-pgsql.yaml', 'r') as file:
            text = file.read()
        text = text.replace('analytics-cl-funnel-to-analytics-pgsql', file_name_pg[:-3].replace('_', '-'))
        f.write(text)
        
    with open(f"deployment/jenkins/production/commands/{file_name[:-3].replace('_', '-')}-values.yaml", 'w') as f:
        text = ''
        with open('deployment/jenkins/production/commands/analytics-cl-funnel-values.yaml', 'r') as file:
            text = file.read()
        text = text.replace('analytics-cl-funnel', file_name[:-3].replace('_', '-'))
        f.write(text)

    with open(f"deployment/jenkins/production/commands/{file_name_pg[:-3].replace('_', '-')}-values.yaml", 'w') as f:
        text = ''
        with open('deployment/jenkins/production/commands/analytics-cl-funnel-to-analytics-pgsql-values.yaml', 'r') as file:
            text = file.read()
        text = text.replace('analytics-cl-funnel-to-analytics-pgsql', file_name_pg[:-3].replace('_', '-'))
        f.write(text)

    text = ''
    new_text = ''
    with open('deployment/jenkins/production/jenkinsfiles/serviceDeployment', 'r') as file:
        text = file.read()
        pat = r"value:'[a-zA-Z,-]*'"
        m = re.search(pat, text)
        vals = m.group(0)
        jobs = vals.split(':')[1][1:-1]
        new_jobs = f"{jobs},{file_name[:-3].replace('_', '-')},{file_name_pg[:-3].replace('_', '-')}"
        new_vals = f"values:'{new_jobs}'"
        new_text = re.sub(pat, new_vals, text)

    with open('deployment/jenkins/production/jenkinsfiles/serviceDeployment', 'w') as f:
        f.write(new_text)