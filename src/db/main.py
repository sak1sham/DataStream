from db.mongo.mongo import MongoMigrate
from db.pgsql.pgsql import PGSQLMigrate
from db.api.api import APIMigrate
from db.kafka.kafka_dms import KafkaMigrate
from notifications.slack_notify import send_message, send_error_message
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
            curr_mapping = mapping for table/collection/api within db
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
            end = time.time()
            time_taken = str(datetime.timedelta(seconds=int(end-start)))
            if('notify' in settings.keys() and settings['notify']):
                msg = f"Migration completed for *{str(self.name)}* from database *{self.db['source']['db_name']}* ({self.db['source']['source_type']}) to *{self.db['destination']['destination_type']}*:\nTotal time taken: {time_taken}\n"
                if(isinstance(result, tuple)):
                    msg += "Insertions: " + "{:,}".format(result[0]) + "\n"
                    msg += "Updations: " + "{:,}".format(result[1])
                try:
                    slack_token = settings['slack_notif']['slack_token']
                    channel = self.curr_mapping['slack_channel'] if 'slack_channel' in self.curr_mapping and self.curr_mapping['slack_channel'] else settings['slack_notif']['channel']
                    send_message(msg = msg, channel = channel, slack_token = slack_token)
                    logger.inform(s="Notification sent successfully.")
                except:
                    logger.err(s = "Unable to connect to slack and send the notification.")
        except Sigterm as e:
            raise
        except Exception as e:
            logger.err(s=traceback.format_exc())
            logger.inform(s = f"{self.curr_mapping['unique_id']}: Migration stopped.\n")
            if('notify' in settings.keys() and settings['notify']):
                msg = f"<!channel> Migration unexpectedly stopped :warning: for *{str(self.name)}* from database *{self.db['source']['db_name']}* ({self.db['source']['source_type']}) to *{self.db['destination']['destination_type']}*"
                error_msg = traceback.format_exc()
                try:
                    slack_token = settings['slack_notif']['slack_token']
                    channel = self.curr_mapping['slack_channel'] if 'slack_channel' in self.curr_mapping and self.curr_mapping['slack_channel'] else settings['slack_notif']['channel']
                    send_error_message(msg = msg, error_msg=error_msg, channel = channel, slack_token = slack_token)
                    logger.inform(s="Notification sent successfully.")
                except:
                    logger.err(s = "Unable to connect to slack and send the notification.")
            