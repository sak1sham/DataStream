from fastapi import APIRouter, Response
from config.migration_mapping import list_databases
from helper_functions.support_functions import fetch_and_convert_data

from pymongo import MongoClient
import certifi

api_router = APIRouter()

@api_router.get("/health", status_code = 200)
async def healthcheck(response: Response):
    pass

@api_router.put("/sync")
async def sync_migration():
    for db in list_databases:
        client = MongoClient(db['url'], tlsCAFile=certifi.where())
        if('collections' not in db.keys()):
            db['collections'] = []
        fetch_and_convert_data(client[db['db_name']], fetch_type=db['fetch_type'], collection_name=db['collections'], db_name=db['db_name'])

