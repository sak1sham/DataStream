# Database Migration Service
## Tech Stack

1. Python version 3.9.1
- Fastapi, Pymongo, pandas, numpy, SQLAlchemy, APScheduler, awswrangler, psycopg2 and other libraries listed in ```requirements.txt```
2. Docker version 20.10.12

## Usage

1. Modify the ```config/migration_mapping.py``` file as per requirements.
Documentation to write the migration_mapping is provided in [Migration Mapping Documentation](config/README.md)

2. Build docker image
```bash
docker build -t migration_service .
```

3. Set up ```.env``` file:

This service might require a temporary database connection to store some encrypted records. For that, connection with a mongodb server is required. AWS credentials are required to access the s3 buckets or redshift.
```
ENCR_MONGO_URL=<Temporary Mongo DB URL to store encrypted records>
DB_NAME=<Name of Temporary Mongo DB>
COLLECTION_NAME=<Name of Temporary Mongo DB Collection>
RUN_MODE=<1 for printing in console or 2 for creating log files>
PORT=<Port to run uvicorn server>
HOST=<Host for uvicorn server>
AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
```

4. Run the docker image while providing the ```.env``` file
```bash
docker run --env-file ./.env migration_service
```
