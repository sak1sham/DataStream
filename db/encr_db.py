from config.migration_mapping import encryption_store
from pymongo import MongoClient
import certifi
from helper.logger import log_writer

def get_data_from_encr_db():
    try:
        client_encr = MongoClient(encryption_store['url'], tlsCAFile=certifi.where())
        db_encr = client_encr[encryption_store['db_name']]
        collection_encr = db_encr[encryption_store['collection_name']]
        log_writer("Successfully connected to encryption database.")
        return collection_encr
    except:
        log_writer("Unable to connect to encryption store database.")
        return None