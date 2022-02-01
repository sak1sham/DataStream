from fastapi import FastAPI
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
import pytz

from config.migration_mapping import mapping
from db.mongo import process_mongodb
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
        year, month, day, week, day_of_week, hour, minute, second = evaluate_cron(db['cron'])
        if(db['source_type'] == 'mongo'):
            scheduler.add_job(process_mongodb, 'cron', args=[db], year=year, month=month, day=day, week=week, day_of_week=day_of_week, hour=hour, minute=minute, second=second, timezone=pytz.timezone('Asia/Calcutta'))
    logger.info('Added jobs to the scheduler.')
    uvicorn.run(app)