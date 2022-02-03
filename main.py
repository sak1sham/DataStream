from fastapi import FastAPI
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import sys

from config.migration_mapping import mapping
from db.mongo import process_mongo_collection
from helper.logger import logger
from helper.util import evaluate_cron

app = FastAPI(title="Migration service")
scheduler = BackgroundScheduler()

@app.on_event("startup")
def scheduled_migration():
    logger.info('Started the scheduler.')
    scheduler.start()

@app.on_event("shutdown")
def end_migration():
    logger.info('Shutting down the scheduler.')
    scheduler.shutdown(wait=False)

@app.get("/health", status_code = 200)
def healthcheck():
    logger.info('Health check done.')
    pass

if __name__ == "__main__":
    for db in mapping:
        if(db['source']['source_type'] == 'mongo'):
            if('collections' not in db.keys()):
                db['collections'] = []
            for curr_collection in db['collections']:
                year, month, day, week, day_of_week, hour, minute, second = evaluate_cron(curr_collection['cron'])
                curr_collection['collection_unique_id'] = db['source']['source_type'] + ":" + db['source']['db_name'] + ":" + curr_collection['collection_name']
                if(len(sys.argv) <= 1 or sys.argv[1] == curr_collection['collection_unique_id']):
                    scheduler.add_job(process_mongo_collection, 'cron', args=[db, curr_collection], id=curr_collection['collection_unique_id'], year=year, month=month, day=day, week=week, day_of_week=day_of_week, hour=hour, minute=minute, second=second, timezone=pytz.timezone('Asia/Calcutta'))
    logger.info('Added job(s) to the scheduler.')
    uvicorn.run(app)