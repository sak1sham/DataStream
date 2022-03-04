from db.mongo import MongoMigrate
from db.pgsql import PGSQLMigrate
from typing import Dict, Any
from helper.logging import logger
import traceback
from notifications.slack_notify import send_message
import config.migration_mapping as config_details

class DMS_importer:
    def __init__(self, db: Dict[str, Any] = {}, curr_mapping: Dict[str, Any] = {}, tz__: str = 'Asia/Kolkata') -> None:
        self.db = db
        self.curr_mapping = curr_mapping
        if(db['source']['source_type'] == 'mongo'):
            self.name = curr_mapping['collection_name']
            self.obj = MongoMigrate(db = db, curr_mapping = curr_mapping, tz_str = tz__)
        elif(db['source']['source_type'] == 'sql'):
            self.name = curr_mapping['table_name']
            self.obj = PGSQLMigrate(db = db, curr_mapping = curr_mapping, tz_str = tz__)
    
    def process(self):
        try:
            self.obj.process()
            if('notify' in config_details.mapping.keys() and config_details.mapping['notify']):
                msg = "Migration completed for *"
                msg += str(self.name)
                msg += "* from database "
                msg += self.db['source']['source_type'] + " : *" + self.db['source']['db_name']
                msg += "*. :tada:"
                try:
                    slack_token = config_details.slack_notif['slack_token']
                    channel = config_details.slack_notif['channel']
                    send_message(msg = msg, channel = channel, slack_token = slack_token)
                    logger.inform("Notification sent successfully.")
                except:
                    logger.err("Unable to connect to slack and send the notification.")
        except Exception as e:
            logger.err(traceback.format_exc())
            logger.inform(self.curr_mapping['unique_id'] + ": Migration stopped.\n")
            if('notify' in config_details.mapping.keys() and config_details.mapping['notify']):
                msg = ":warning: Migration unexpectedly stopped for *"
                msg += str(self.name)
                msg += "* from database "
                msg += self.db['source']['source_type'] + " : *" + self.db['source']['db_name']
                msg += "*. :warning:"
                try:
                    slack_token = config_details.slack_notif['slack_token']
                    channel = config_details.slack_notif['channel']
                    send_message(msg = msg, channel = channel, slack_token = slack_token)
                    logger.inform("Notification sent successfully.")
                except:
                    logger.err("Unable to connect to slack and send the notification.")
            