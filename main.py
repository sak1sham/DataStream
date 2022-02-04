from fastapi import FastAPI
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import sys

from config.migration_mapping import mapping
from db.mongo import process_mongo_collection
from db.sql import process_sql_table
from helper.logger import log_writer
from helper.util import evaluate_cron

import os
from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="Migration service")
scheduler = BackgroundScheduler()

@app.on_event("startup")
def scheduled_migration():
    log_writer('Started the scheduler.')
    scheduler.start()

@app.on_event("shutdown")
def end_migration():
    log_writer('Shutting down the scheduler.')
    scheduler.shutdown(wait=False)

@app.get("/health", status_code = 200)
def healthcheck():
    log_writer('Health check done.')
    pass

if __name__ == "__main__":
    for db in mapping:
        if(db['source']['source_type'] == 'sql'):
            if('tables' not in db.keys()):
                db['tables'] = []
            for curr_table in db['tables']:
                year, month, day, week, day_of_week, hour, minute, second = evaluate_cron(curr_table['cron'])
                curr_table['table_unique_id'] = db['source']['source_type'] + ":" + db['source']['db_name'] + ":" + curr_table['table_name']
                if(curr_table['table_unique_id'] in sys.argv[1:]):
                    scheduler.add_job(process_sql_table, 'cron', args=[db, curr_table], id=curr_table['table_unique_id'], year=year, month=month, day=day, week=week, day_of_week=day_of_week, hour=hour, minute=minute, second=second, timezone=pytz.timezone('Asia/Calcutta'))
        if(db['source']['source_type'] == 'mongo'):
            continue
            if('collections' not in db.keys()):
                db['collections'] = []
            for curr_collection in db['collections']:
                year, month, day, week, day_of_week, hour, minute, second = evaluate_cron(curr_collection['cron'])
                curr_collection['collection_unique_id'] = db['source']['source_type'] + ":" + db['source']['db_name'] + ":" + curr_collection['collection_name']
                if(curr_collection['collection_unique_id'] in sys.argv[1:]):
                    scheduler.add_job(process_mongo_collection, 'cron', args=[db, curr_collection], id=curr_collection['collection_unique_id'], year=year, month=month, day=day, week=week, day_of_week=day_of_week, hour=hour, minute=minute, second=second, timezone=pytz.timezone('Asia/Calcutta'))
    log_writer('Added job(s) to the scheduler.')
    uvicorn.run(app, port=int(os.getenv('PORT')), host=os.getenv("HOST"))