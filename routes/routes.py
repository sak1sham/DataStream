from fastapi import APIRouter, Response
from config.database import client
from config.migration_properties import list_databases
from routes.support_functions import fetch_and_convert_data

api_router = APIRouter()

@api_router.get("/health", status_code = 200)
async def healthcheck(response: Response):
    try:
        abc = client.server_info()
        print(abc)
    except:
        response.status_code = 503


@api_router.get("/fetchRecords")
async def fetch_records(query: str):
    return []


@api_router.put("/sync")
async def sync_migration():
    for db in list_databases:
        if(db['fetch_type'] == 'selected' and 'collections' not in db.keys()):
            db['collections'] = []
        fetch_and_convert_data(client[db['db_name']], fetch_type=db['fetch_type'], collection_name=db['collections'])

