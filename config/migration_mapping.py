import os
import datetime
import pytz
IST_tz = pytz.timezone('Asia/Kolkata')

from dotenv import load_dotenv
load_dotenv()

mapping = [
    {
        'source': {
            'source_type': 'sql',
            'url': 'cmdb-rr.cbo3ijdmzhje.ap-south-1.rds.amazonaws.com',
            'db_name': 'cmdb',
            'username': 'cm',
            'password': 'cm'
        },
        'destination': {
            'destination_type': 's3',
            's3_bucket_name': 'migration-service-temp-2'
        },
        'tables':[
            {
                'table_name': 'localities_live',
                'cron': '* * * * * * */1 0',
                'to_partition': True,
                'partition_col': 'migration_snapshot_date',
                'is_dump': True
            }
        ]
    }
]

encryption_store = {
    'url': os.getenv('ENCR_MONGO_URL'),
    'db_name': os.getenv('DB_NAME'),
    'collection_name': os.getenv('COLLECTION_NAME')
}