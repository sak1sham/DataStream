# DataStream

## Introduction

A Data Migration Service (DMS) with multiple capabilities for creating pipelines seamlessly:
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

A ```requirements.txt``` file is first created, and then all required installations are performed.

Once the installations are done, we are just 1 step away from starting creating our data pipelines.

### Configuring settings

This DMS can be customized as per the requirements by creating a settings file (```src/config/settings.py```). Check out [this sample file](sample_config/settings.py) to learn how to create your own settings. The meaning for each settings field is documented [here](src/config/README.md).

### Creating Data Pipelines

Check out [this documentation](src/config/README.md) to learn to create your own new data pipelines. A new data pipeline is created by adding a new file inside ```src/config/jobs/``` with a ```.py``` extension. The name of the file represents the unique id for the pipeline. Some sample mappings are present inside the ```src/config/jobs/``` folder.

### Starting the Migration

Now suppose you have created the following job mappings for your data pipelines: ```job_1.py```, ```job_2.py```, ```job_3.py```. You can start your migration by calling the ```main.py``` script from within the ```src``` folder. Pass in the file name(s) of the job(s) to be run as command line arguments.

```python
## To run (or scheduled run) a single data pipeline: job_1.py
python main.py job_1
```

```python
## To run (or scheduled run) the job_1.py, job_2.py and job_3.py data pipelines together
python main.py job_1 job_2 job_3
```