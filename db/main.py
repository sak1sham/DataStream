from db.mongo import MongoMigrate
from db.pgsql import PGSQLMigrate
from db.s3 import S3Migrate
from db.kafka_dms import KafkaMigrate

from typing import Dict, Any
from helper.exceptions import IncorrectMapping
from helper.logger import logger
import traceback
from notifications.slack_notify import send_message
from config.settings import settings

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
        elif(db['source']['source_type'] == 'sql'):
            self.name = curr_mapping['table_name']
            self.obj = PGSQLMigrate(db = db, curr_mapping = curr_mapping, tz_str = tz__)
        elif(db['source']['source_type'] == 's3'):
            self.name = curr_mapping['table_name']
            self.obj = S3Migrate(db = db, curr_mapping = curr_mapping, tz_str = tz__)
        elif(db['source']['source_type'] == 'kafka'):
            self.name = curr_mapping['topic_name']
            self.obj = KafkaMigrate(db = db, curr_mapping = curr_mapping, tz_str = tz__)
        else:
            raise IncorrectMapping("source_type can be sql, mongo, s3 or kafka")
    
    def process(self):
        try:
            #self.obj.process()
            if('notify' in settings.keys() and settings['notify']):
                msg = "Migration completed for *"
                msg += str(self.name)
                msg += "* from database "
                msg += self.db['source']['source_type'] + " : *" + self.db['source']['db_name']
                msg += "*. :tada:"
                try:
                    slack_token = settings['slack_notif']['slack_token']
                    channel = settings['slack_notif']['channel']
                    send_message(msg = msg, channel = channel, slack_token = slack_token)
                    logger.inform(s="Notification sent successfully.")
                except:
                    logger.err(s=("Unable to connect to slack and send the notification."))
        except Exception as e:
            logger.err(s=traceback.format_exc())
            logger.inform(s=(self.curr_mapping['unique_id'] + ": Migration stopped.\n"))
            if('notify' in settings.keys() and settings['notify']):
                msg = "Migration unexpectedly stopped for *"
                msg += str(self.name)
                msg += "* from database "
                msg += self.db['source']['source_type'] + " : *" + self.db['source']['db_name']
                msg += "*. :warning:"
                try:
                    slack_token = settings['slack_notif']['slack_token']
                    channel = settings['slack_notif']['channel']
                    send_message(msg = msg, channel = channel, slack_token = slack_token)
                    logger.inform(s="Notification sent successfully.")
                except:
                    logger.err(s=("Unable to connect to slack and send the notification."))
            