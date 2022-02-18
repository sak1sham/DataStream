# Database Migration Service
## Tech Stack

1. Python version 3.9.1
- Fastapi, Pymongo, pandas, numpy, SQLAlchemy, APScheduler, awswrangler, psycopg2 and other libraries listed in ```requirements.txt```
2. Docker version 20.10.12

## Usage

1. Modify the ```config/migration_mapping.py``` file as per requirements.
Documentation to write the migration_mapping is provided in [Migration Mapping Documentation](config/README.md)

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