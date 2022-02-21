from fastapi import FastAPI
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import sys
import random

from config.migration_mapping import mapping
from db.mongo import process_mongo_collection
from db.sql import process_sql_table
import logging
from helper.util import evaluate_cron

import os
from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="Migration service")
scheduler = BackgroundScheduler()

@app.on_event("startup")
def scheduled_migration():
    logging.info('Started the scheduler.')
    scheduler.start()

@app.on_event("shutdown")
def end_migration():
    logging.info('Shutting down the scheduler.')
    scheduler.shutdown(wait=False)

@app.get("/health", status_code = 200)
def healthcheck():
    logging.info('Health check done.')
    pass

def schedule_new_job(db, list_specs, uid, i, custom_f):
    list_specs['unique_id'] = uid + "_MIGRATION_SERVICE_" + str(i+1)
    if(list_specs['cron'] == 'run'):
        custom_f(db, list_specs)
    else:
        year, month, day, week, day_of_week, hour, minute, second = evaluate_cron(list_specs['cron'])
        scheduler.add_job(custom_f, 'cron', args=[db, list_specs], id=list_specs['unique_id'], year=year, month=month, day=day, week=week, day_of_week=day_of_week, hour=hour, minute=minute, second=second, timezone=pytz.timezone('Asia/Kolkata'))

def use_mapping(db, key, custom_f):
    if(key not in db.keys()):
        db[key] = []
    for i, curr_mapping in enumerate(db[key]):
        schedule_new_job(db, curr_mapping, unique_id, i, custom_f)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    custom_records_to_run = sys.argv[1:]
    for unique_id, db in mapping.items():
        if(unique_id == 'fastapi_server'):
            continue
        elif(db['source']['source_type'] == 'sql'):
            use_mapping(db, 'tables', process_sql_table)
        elif(db['source']['source_type'] == 'mongo'):
            use_mapping(db, 'collections', process_mongo_collection)
        else:
            logging.error("Un-identified Source Type " + str(db['source']['source_type']) + " found in migration-mapping.")
    logging.info('Added all jobs.')
    if('fastapi_server' in mapping.keys() and mapping['fastapi_server']):
        uvicorn.run(app, port=int(os.getenv('PORT')), host=os.getenv("HOST"))