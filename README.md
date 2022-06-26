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

This DMS can be customized as per the requirements by creating a settings file (```src/config/settings.py```). For a sample on how to create a settings file, check out [this file](sample_config/settings.py). The meaning for each settings field is present [here](src/config/README.md).

### Creating Data Pipelines

The documentation to create new data pipelines is present [here](src/config/README.md). A new data pipeline is created by adding a new file inside ```src/config/jobs/``` with a ```.py``` extension. The name of the file represents the unique id for the pipeline. Some sample mappings are present inside the ```src/config/jobs/``` folder.

