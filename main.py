from fastapi import FastAPI
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import sys

from config.migration_mapping import mapping
from db.main import central_processer
from helper.logging import logger
from helper.util import evaluate_cron

import os
from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="Migration service")
scheduler = BackgroundScheduler()

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

def create_new_job(db, list_specs, uid, i, is_fastapi):
    list_specs['unique_id'] = uid + "_MIGRATION_SERVICE_" + str(i+1)
    if(list_specs['cron'] == 'run'):
        central_processer(db, list_specs)
    elif(is_fastapi):
        year, month, day, week, day_of_week, hour, minute, second = evaluate_cron(list_specs['cron'])
        scheduler.add_job(central_processer, 'cron', args=[db, list_specs], id=list_specs['unique_id'], year=year, month=month, day=day, week=week, day_of_week=day_of_week, hour=hour, minute=minute, second=second, timezone=pytz.timezone('Asia/Kolkata'))
    else:
        logger.warn("Jobs can be scheduled only if fastapi_server is enabled. Skipping " + str(uid) + ".")

def use_mapping(db, key, is_fastapi):
    if(key not in db.keys()):
        db[key] = []
    for i, curr_mapping in enumerate(db[key]):
        create_new_job(db, curr_mapping, unique_id, i, is_fastapi)

if __name__ == "__main__":
    custom_records_to_run = sys.argv[1:]
    is_fastapi = False
    if('fastapi_server' in mapping.keys() and mapping['fastapi_server']):
        is_fastapi = True
    for unique_id, db in mapping.items():
        if(unique_id == 'fastapi_server'):
            continue
        s_type = db['source']['source_type']
        if(s_type == 'sql'):
            use_mapping(db, 'tables', is_fastapi)
        elif(s_type == 'mongo'):
            use_mapping(db, 'collections', is_fastapi)
        else:
            logger.err("Un-identified Source Type " + str(db['source']['source_type']) + " found in migration-mapping.")
    logger.inform('Added all jobs.')
    if(is_fastapi):
        uvicorn.run(app, port=int(os.getenv('PORT')), host=os.getenv("HOST"))