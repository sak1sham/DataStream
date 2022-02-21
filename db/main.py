from db.mongo import process_mongo_collection
from db.sql import process_sql_table

class Central_processing_unit:
    def __init__(self, db, curr_mapping) -> None:
        if(db['source']['source_type'] == 'mongo'):
            process_mongo_collection(db, curr_mapping)
        elif(db['source']['source_type'] == 'sql'):
            process_sql_table(db, curr_mapping)
    
def central_processer(db, curr_mapping):
    if(db['source']['source_type'] == 'mongo'):
        process_mongo_collection(db, curr_mapping)
    elif(db['source']['source_type'] == 'sql'):
        process_sql_table(db, curr_mapping)