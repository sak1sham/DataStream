from db.mongo.mongo import MongoMigrate
from db.pgsql.pgsql import PGSQLMigrate
from db.api.api import APIMigrate
from db.kafka.kafka_dms import KafkaMigrate
from notifications.slack_notify import send_formatted_message
from config.settings import settings
from helper.exceptions import IncorrectMapping, Sigterm
from helper.logger import logger

from typing import Dict, Any
import traceback
import time
import datetime

class DMS_importer:
    def __init__(self, db: Dict[str, Any] = {}, curr_mapping: Dict[str, Any] = {}, tz__: str = 'Asia/Kolkata') -> None:
        '''
            db = complete mapping for a particular job_id
            curr_mapping = mapping for table/collection/api/topic within db
            tz__ = timezone
        '''
        self.db = db
        self.curr_mapping = curr_mapping
        if(db['source']['source_type'] == 'mongo'):
            self.name = curr_mapping['collection_name']
            self.obj = MongoMigrate(db = db, curr_mapping = curr_mapping, tz_str = tz__)
        elif(db['source']['source_type'] == 'pgsql'):
            self.name = curr_mapping['table_name']
            self.obj = PGSQLMigrate(db = db, curr_mapping = curr_mapping, tz_str = tz__)
        elif(db['source']['source_type'] == 'api'):
            self.name = curr_mapping['api_name']
            self.obj = APIMigrate(db = db, curr_mapping = curr_mapping, tz_str = tz__)
        elif(db['source']['source_type'] == 'kafka'):
            self.name = curr_mapping['topic_name']
            self.obj = KafkaMigrate(db = db, curr_mapping = curr_mapping, tz_str = tz__)
        else:
            raise IncorrectMapping("source_type can be api, pgsql, mongo or kafka")
    
    def process(self):
        try:
            start = time.time()
            result = self.obj.process()
            time_taken = str(datetime.timedelta(seconds=int(time.time()-start)))
            if('notify' in settings.keys() and settings['notify']):
                if(not isinstance(result, tuple)):
                    result = (-1, -1)
                try:
                    send_formatted_message(
                        channel = self.curr_mapping['slack_channel'] if 'slack_channel' in self.curr_mapping and self.curr_mapping['slack_channel'] else settings['slack_notif']['channel'],
                        slack_token = settings['slack_notif']['slack_token'],
                        status = True, 
                        name = self.name, 
                        database = self.db['source']['db_name'], 
                        db_type = self.db['source']['source_type'], 
                        destination = self.db['destination']['destination_type'], 
                        time = time_taken,
                        insertions = result[0], 
                        updations = result[1],
                    )
                    logger.inform(s="Notification sent successfully.")
                except:
                    logger.err(s = "Unable to connect to slack and send the notification.")
        except Sigterm as e:
            raise
        except Exception as e:
            logger.err(s=traceback.format_exc())
            logger.inform(s = f"{self.curr_mapping['unique_id']}: Migration stopped.\n")
            if('notify' in settings.keys() and settings['notify']):
                try:
                    send_formatted_message(
                        channel = self.curr_mapping['slack_channel'] if 'slack_channel' in self.curr_mapping and self.curr_mapping['slack_channel'] else settings['slack_notif']['channel'],
                        slack_token = settings['slack_notif']['slack_token'],
                        status = False, 
                        name = self.name, 
                        database = self.db['source']['db_name'], 
                        db_type = self.db['source']['source_type'], 
                        destination = self.db['destination']['destination_type'], 
                        thread = traceback.format_exc() 
                    )
                    logger.inform(s="Notification sent successfully.")
                except:
                    logger.err(s = "Unable to connect to slack and send the notification.")
            