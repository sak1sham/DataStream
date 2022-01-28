from fastapi import FastAPI
import logging
import uvicorn

from config.migration_mapping import list_databases
from helper.util import create_directories
from apscheduler.schedulers.background import BackgroundScheduler
from mongo.util import process_db

logger = logging.getLogger(__name__)
app = FastAPI(title="Migration service")
scheduler = BackgroundScheduler()

@app.on_event("startup")
def scheduled_migration():
    scheduler.start()

@app.on_event("shutdown")
def end_migration():
    scheduler.shutdown(wait=False)

@app.get("/health", status_code = 200)
def healthcheck():
    pass

if __name__ == "__main__":
    for db in list_databases:
        scheduler.add_job(process_db, 'cron', args=[db], hour=db['time_hour'], minute=db['time_minute'])
    create_directories(list_databases)
    uvicorn.run(app)