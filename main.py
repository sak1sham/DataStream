from fastapi import FastAPI
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import sys
from typing import Tuple, Dict, Any
import threading

from config.migration_mapping import mapping
from db.main import DMS_importer
from helper.logging import logger
from helper.util import evaluate_cron
from helper.exceptions import InvalidArguments, SourceNotFound

import os
from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="Migration service")
scheduler = BackgroundScheduler()
group_key = {
    'sql': 'tables',
    'mongo': 'collections',
    'api': 'apis'
}
tz__ = 'Asia/Kolkata'
reserved_mapping_keys = ['fastapi_server', 'timezone']

@app.on_event("startup")
def scheduled_migration():
    logger.inform('Started the scheduler.')
    scheduler.start()

@app.on_event("shutdown")
def end_migration():
    logger.inform('Shutting down the scheduler.')
    scheduler.shutdown(wait=False)

@app.get("/health", status_code = 200)
def healthcheck():
    logger.inform('Health check done.')
    pass

def migration_service_of_job(db: Dict[str, Any] = {}, curr_mapping: Dict[str, Any] = {}, tz__: str = 'Asia/Kolkata') -> None:
    obj = DMS_importer(db, curr_mapping)
    obj.process()

def create_new_job(db, list_specs, uid, i, is_fastapi):
    list_specs['unique_id'] = uid + "_MIGRATION_SERVICE_" + str(i+1)
    if(list_specs['cron'] == 'self-managed'):
        th = threading.Thread(target=migration_service_of_job, args=(db, list_specs, tz__))
        th.start()
    elif(is_fastapi):
        year, month, day, week, day_of_week, hour, minute, second = evaluate_cron(list_specs['cron'])
        scheduler.add_job(migration_service_of_job, 'cron', args=[db, list_specs, tz__], id=list_specs['unique_id'], year=year, month=month, day=day, week=week, day_of_week=day_of_week, hour=hour, minute=minute, second=second, timezone=pytz.timezone(tz__))
    else:
        logger.warn("Jobs can be scheduled only if fastapi_server is enabled. Skipping " + str(uid) + ".")

def use_mapping(db, key, is_fastapi):
    if(key not in db.keys()):
        db[key] = []
    for i, curr_mapping in enumerate(db[key]):
        create_new_job(db, curr_mapping, unique_id, i, is_fastapi)

def get_batch_size(s) -> Tuple[int]:
    b = s.split(',')
    b = [int(x) for x in b]
    if(len(b) != 3):
        raise InvalidArguments("Batch (\'-b\' or \'-batch\') shall be provided in format \'X,Y,Z\' without quotes, without any spaces. X = index of target mapping for the unique_job_id. Y and Z represent start (inclusive) and end (exclusive) of table numbers to fetch. Batches are used only when all tables of the database are fetched.")
    return b[0], b[1], b[2]

if __name__ == "__main__":
    args = sys.argv[1:]
    is_fastapi = False
    if('fastapi_server' in mapping.keys() and mapping['fastapi_server']):
        is_fastapi = True
    if('timezone' in mapping.keys() and mapping['timezone']):
        tz__ = mapping['timezone']
    n = len(args)
    if(n > 0):
        ## If some command line arguments are provided, process only that data
        i = 0
        while(i < n):
            unique_id = args[i]
            if(unique_id in reserved_mapping_keys):
                raise InvalidArguments("Can\'t use fastapi_server, timezone as job id. It is a reserved keyword.")
            db = mapping[unique_id]
            s_type = db['source']['source_type']
            if(s_type not in group_key.keys()):
                raise SourceNotFound("Un-identified Source Type " + str(db['source']['source_type']) + " found in migration-mapping.")
            if(i <= n-3 and (args[i+1] == '-b' or args[i+1] == '-batch')):
                b_mapping_number, b_start, b_end = get_batch_size(args[i+2])
                curr_map = db[group_key[s_type]][b_mapping_number]
                curr_map['batch_start'] = b_start
                curr_map['batch_end'] = b_end
                i += 2
            use_mapping(db, group_key[s_type], is_fastapi)
            i += 1
    else:
        for unique_id, db in mapping.items():
            if(unique_id in reserved_mapping_keys):
                continue
            s_type = db['source']['source_type']
            if(s_type not in group_key.keys()):
                raise SourceNotFound("Un-identified Source Type " + str(db['source']['source_type']) + " found in migration-mapping.")
            use_mapping(db, group_key[s_type], is_fastapi)
    logger.inform('Added all jobs.')
    if(is_fastapi):
        uvicorn.run(app, port=int(os.getenv('PORT')), host=os.getenv("HOST"))
