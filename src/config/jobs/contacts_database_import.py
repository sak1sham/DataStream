import os
from dotenv import load_dotenv
load_dotenv()

mapping = {
    "destination": {
        "s3": {
            "destination_type": "s3",
            "s3_bucket_name": "database-migration-service-prod"
        }
    },
    "source": {
        "db_name": "cmdb",
        "password": os.getenv('DB_PASSWORD'),
        "source_type": "pgsql",
        "url": os.getenv('CMDB_URL'),
        "username": os.getenv('DB_USERNAME')
    },
    "tables": [
        {
            "batch_size": 10000,
            "cron": "self-managed",
            "mode": "dumping",
            "table_name": "contacts_database_import"
        }
    ]
}