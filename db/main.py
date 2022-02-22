from db.mongo import process_mongo_collection
from db.pgsql import process_sql_table
from typing import Dict, Any

class Central_processing_unit:
    def __init__(self, db: Dict[str, Any] = {}, curr_mapping: Dict[str, Any] = {}) -> None:
        if(db['source']['source_type'] == 'mongo'):
            process_mongo_collection(db, curr_mapping)
        elif(db['source']['source_type'] == 'sql'):
            process_sql_table(db, curr_mapping)
    
def central_processer(db: Dict[str, Any] = {}, curr_mapping: Dict[str, Any] = {}) -> None:
    if(db['source']['source_type'] == 'mongo'):
        process_mongo_collection(db, curr_mapping)
    elif(db['source']['source_type'] == 'sql'):
        process_sql_table(db, curr_mapping)