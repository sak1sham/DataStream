from db.mongo import MongoMigrate
from db.pgsql import PGSQLMigrate
from typing import Dict, Any
from helper.logging import logger
import traceback

class DMS_importer:
    def __init__(self, db: Dict[str, Any] = {}, curr_mapping: Dict[str, Any] = {}) -> None:
        self.db = db
        self.curr_mapping = curr_mapping
        if(db['source']['source_type'] == 'mongo'):
            self.obj = MongoMigrate(db, curr_mapping)
        elif(db['source']['source_type'] == 'sql'):
            self.obj = PGSQLMigrate(db, curr_mapping)
    
    def process(self):
        try:
            self.obj.process()
        except Exception as e:
            logger.err(traceback.format_exc())
            logger.inform(self.curr_mapping['unique_id'] + ": Migration stopped.\n")