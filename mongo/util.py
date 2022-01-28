from pymongo import MongoClient
from helper.util import fetch_and_convert_data
import certifi

def process_db(db):
    print('Time to connect to db', db['db_name'])
    client = MongoClient(db['url'], tlsCAFile=certifi.where())
    if('collections' not in db.keys()):
        db['collections'] = []
    fetch_and_convert_data(client[db['db_name']], fetch_type=db['fetch_type'], collection_name=db['collections'], db_name=db['db_name'])
