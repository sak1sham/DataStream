from fastapi import FastAPI
import uvicorn
from config.migration_mapping import mapping
from storage.local import create_directories
from helper.logger import logger
from apscheduler.schedulers.background import BackgroundScheduler
import pytz

app = FastAPI(title="Migration service")
scheduler = BackgroundScheduler()

@app.on_event("startup")
def scheduled_migration():
    logger.info("Starting the scheduler.")
    scheduler.start()

@app.on_event("shutdown")
def end_migration():
    scheduler.shutdown(wait=False)
    logger.info("Shutting down scheduler and deleting all jobs.")

@app.get("/health", status_code = 200)
def healthcheck():
    logger.info("Health-check performed.")
    pass

if __name__ == "__main__":
    create_directories(mapping)
    logger.info("Created the directories.")
    from db.mongo import process_mongodb
    from helper.util import evaluate_cron
    for db in mapping:
        year, month, day, week, day_of_week, hour, minute, second = evaluate_cron(db['cron'])
        if(db['source_type'] == 'mongo'):
            scheduler.add_job(process_mongodb, 'cron', args=[db], year=year, month=month, day=day, week=week, day_of_week=day_of_week, hour=hour, minute=minute, second=second, timezone=pytz.timezone('Asia/Calcutta'))
    logger.info("Added all jobs to scheduler. Now, attempting to run the fastapi application.")
    uvicorn.run(app)