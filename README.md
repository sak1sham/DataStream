# Data Migration Service

## Introduction

A migration service with multiple capabilities for creating data pipelines seamlessly:
1. Migrate data from PgSQL, MongoDB, API services, Kafka Consumer
2. Store data into Amazon S3, Redshift or a PgSQL database
3. 3 Modes of operation (Dumping, Logging and Syncing)
4. Internal or external pipeline scheduling
5. Data cleaning and correction

This script is written in ```Python 3.8.0```

## Usage

### Setting things up

As a first step, we need to set up some things. 
1. Install version ```21.2.4``` of pip, 
2. Install the command line interface tools for Amazon web services (AWS)
3. Install pip-tools for keeping track of python dependencies

```
pip3 install --upgrade pip==21.2.4
pip3 --no-cache-dir install --upgrade awscli
pip3 install pip-tools
```

Next, we need to setup the environment by installing the required dependencies for the script.
```
pip-compile
pip-sync
```

These commands with first create a ```requirements.txt``` file, and then do the required installations.

Once the installations are done, we are just 1 step away from starting creating our data pipelines.

### Configuring settings

This DMS can be customized as per the requirements by creating a settings file (``src/config/settings.py````). Here is a sample settings file:

```python
import os
from dotenv import load_dotenv
load_dotenv()
import datetime
import pytz

settings = {
    'fastapi_server': False,
    'timezone': 'Asia/Kolkata',
    'notify': True,
    'encryption_store': {
        'url': os.getenv('ENCR_MONGO_URL'),
        'db_name': os.getenv('DB_NAME'),
        'collection_name': os.getenv('COLLECTION_NAME')
    },
    'dashboard_store': {
        'url': os.getenv('ENCR_MONGO_URL'),
        'db_name': os.getenv('DB_NAME'),
        'collection_name': 'dms_history'
    },
    'slack_notif': {
        'slack_token': 'x**Z-88*********7-421*********6-F0x****************xv3Z',
        'channel': "C042ANGF320"
    },
    'cut_off_time': datetime.time(hour=9, minute=0, second=0, microsecond=0, tzinfo=pytz.timezone('Asia/Kolkata')),
}
```


### Creating Data Pipelines

All configuration files are stored inside folder ```src/config```
1. The Data Pipelines are created and stored inside folder ```src/config/jobs```
2. The general script settings can be customized in ```src/config/settings.py``` file

The documentation to create Data Pipelines is present [here](src/config/README.md).


2. Modify the ```CMD``` command in ```Dockerfile``` as per requirements. Also, set the environment variables in ```docker-compose.yml``` file.
```
ENCR_MONGO_URL=<Temp_Mongo_DB_URL>
DB_NAME=<Temp_Mongo_DB_Name>
COLLECTION_NAME=<Temp_Mongo_DB_Collection_Name>
PORT=<Uvicorn_Server_Port>
HOST=<Uvicorn_Server_Host>
AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
```

- ENCR_MONGO_URL, DB_NAME, COLLECTION_NAME: Some records from your source_database are saved temprarily (SHA256 encrypted) in a temporary mongoDB collection. These parameters specify the credentials and names for that temporary Mongo Database.
- PORT, HOST: needed to run uvicorn server
- AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY: Credentials to save files to destination.

3. Use docker-compose to run service
```bash
docker-compose build
```
```bash
docker-compose up
```

This system can convert common datetime string columns to datetime columns.

## DEPLOYMENT ON KUBERNETES

1. create a new docker file in deployment/dockerfiles
2. update the path of the new docker file created in github workflows
3. create a new workflow file with different name and app name in the file
4. update the environment variables in the deployment/env/prod/config.production.yaml


## Visualizing jobs through DMS Dashboard
1. ```pip install streamlit```
2. ```cd src```
3. ```streamlit run dashboard.py```