from fastapi import FastAPI
import uvicorn

from config.migration_mapping import mapping
from storage.local import create_directories

from apscheduler.schedulers.background import BackgroundScheduler
from mongo.util import process_db
from helper.util import evaluate_cron

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
    create_directories(mapping)
    for db in mapping:
        year, month, day, week, day_of_week, hour, minute, second = evaluate_cron(db['cron'])
        scheduler.add_job(process_db, 'cron', args=[db], year=year, month=month, day=day, week=week, day_of_week=day_of_week, hour=hour, minute=minute, second=second)
    uvicorn.run(app)