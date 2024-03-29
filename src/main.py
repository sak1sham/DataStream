from fastapi import FastAPI
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import sys
from typing import Tuple, Dict, Any
import traceback

from config.migration_mapping import get_mapping
from config.settings import settings
from db.main import DMS_importer
from helper.logger import logger
from helper.util import evaluate_cron
from helper.exceptions import InvalidArguments, SourceNotFound, Sigterm
from routes.routes import router

import os
from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="Migration service")
app.include_router(router=router)
scheduler = BackgroundScheduler()
group_key = {
    'pgsql': 'tables',
    'mongo': 'collections',
    'api': 'apis',
    's3': 'tables',
    'kafka': 'topics'
}

tz__ = 'Asia/Kolkata'

@app.on_event("startup")
def scheduled_migration():
    logger.inform(s='Started the scheduler.')
    scheduler.start()

@app.on_event("shutdown")
def end_migration():
    logger.inform(s='Shutting down the scheduler.')
    scheduler.shutdown(wait=False)

@app.get("/health", status_code = 200)
def healthcheck():
    logger.inform(s='Health check done.')
    pass

def migration_service_of_job(db: Dict[str, Any] = {}, curr_mapping: Dict[str, Any] = {}, tz__: str = 'Asia/Kolkata') -> None:
    obj = DMS_importer(db=db, curr_mapping=curr_mapping, tz__=tz__)
    obj.process()


def create_new_job(db, list_specs, uid, is_fastapi):
    specs_name_type = f"{group_key[db['source']['source_type']][:-1]}_name"
    basic_unique_id = f"{uid}_DMS_{list_specs[specs_name_type]}"
    list_specs['unique_id'] = basic_unique_id
    list_destinations = db['destination']
    db_copy = db.copy()
    try:
        for key, destination in list_destinations.items():
            list_specs['unique_id'] = f"{basic_unique_id}_{key}"
            db_copy['destination'] = {}
            for key_ in destination.keys():
                db_copy['destination'][key_] = destination[key_]
            if(list_specs['cron'] == 'self-managed'):
                migration_service_of_job(db_copy, list_specs, tz__)
            elif(is_fastapi):
                year, month, day, week, day_of_week, hour, minute, second = evaluate_cron(list_specs['cron'])
                scheduler.add_job(migration_service_of_job, 'cron', args=[db_copy, list_specs, tz__], id=list_specs['unique_id'], year=year, month=month, day=day, week=week, day_of_week=day_of_week, hour=hour, minute=minute, second=second, timezone=pytz.timezone(tz__), misfire_grace_time=None)
            else:
                logger.warn(s = f"Jobs can be scheduled only if fastapi_server is enabled. Skipping {str(uid)}.")
    except Sigterm as e:
        logger.err(s=traceback.format_exc())
        logger.inform(s = f"{basic_unique_id}: Migration stopped.\n")
        

def use_mapping(db, key, is_fastapi):
    if(key not in db.keys()):
        db[key] = []
    for curr_mapping in db[key]:
        create_new_job(db, curr_mapping, unique_id, is_fastapi)

def get_batch_size(s) -> Tuple[int]:
    b = s.split(',')
    b = [int(x) for x in b]
    if(len(b) != 3):
        raise InvalidArguments("Batch (\'-b\' or \'-batch\') shall be provided in format \'X,Y,Z\' without any quotes or spaces. X = index of target mapping for the unique_job_id. Y and Z represent start (inclusive) and end (exclusive) of table numbers to fetch. Batches are used only when all tables of the database are fetched.")
    return b[0], b[1], b[2]

if __name__ == "__main__":
    args = sys.argv[1:]
    is_fastapi = True if 'fastapi_server' in settings.keys() and settings['fastapi_server'] else False
    if('timezone' in settings.keys() and settings['timezone']):
        tz__ = settings['timezone']
    n = len(args)
    if(n > 0):
        if(args[0] == "__test__"):
            logger.inform("Testing: successful.")
            logger.inform("You are now ready to customize and run this migration service, by add mappings to config/jobs/ and modifying config/settings.py as per requirements.")
            exit(0)
        ## If some command line arguments are provided, process only that data
        i = 0
        while(i < n):
            unique_id = args[i]
            db = get_mapping(unique_id)
            db['id'] = unique_id
            s_type = db['source']['source_type']
            if(s_type not in group_key.keys()):
                raise SourceNotFound(f"Un-identified Source Type {str(db['source']['source_type'])} found in migration-mapping.")
            if(i <= n-3 and (args[i+1] == '-b' or args[i+1] == '-batch')):
                b_mapping_number, b_start, b_end = get_batch_size(args[i+2])
                curr_map = db[group_key[s_type]][b_mapping_number]
                curr_map['batch_start'] = b_start
                curr_map['batch_end'] = b_end
                i += 2
            use_mapping(db, group_key[s_type], is_fastapi)
            i += 1
        logger.inform(s='Added all jobs.')
    else:
        logger.inform(s="Please provide the job_id as arguments to migrate")
    if(is_fastapi):
        uvicorn.run(app, port=int(os.getenv('PORT')), host=os.getenv("HOST"))
