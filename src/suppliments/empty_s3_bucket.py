import awswrangler as wr
import sys
from dotenv import load_dotenv
load_dotenv()
import traceback

from settings import settings
from slack_notify import send_message
from logger import logger

if __name__ == "__main__":
    args = sys.argv[1:]
    if(len(args) > 0):
        try:
            logger.inform(s = "Deleting data from bucket {0}".format(args[0]))
            # "aws-athena-query-results-414085459896-ap-south-1"
            path = "s3://{0}".format(args[0])
            logger.inform(s = "Deleting data from path {0}".format(path))
            objects_for_deletion = wr.s3.list_objects(path=path)
            count = 0
            for obj in objects_for_deletion:
                wr.s3.delete_objects(path=obj)
                count += 1
            logger.inform(s = "Deleted {0} objects from bucket {1}.".format("{:,}".format(count), args[0]))
            if('notify' in settings.keys() and settings['notify']):
                send_message(msg = "Deleted *{0}* objects from S3 bucket *{1}*.".format("{:,}".format(count), args[0]), channel=settings['slack_notif']['channel'], slack_token=settings['slack_notif']['slack_token'])
                logger.inform(s = "Notification sent.")
        except Exception as e:
            logger.err(traceback.format_exc())
            if('notify' in settings.keys() and settings['notify']):
                send_message(msg = "Caught an exception while empyting bucket *{0}*:\n```{1}```".format(args[0], traceback.format_exc()), channel=settings['slack_notif']['channel'], slack_token=settings['slack_notif']['slack_token'])
                logger.inform(s = "Notification sent.")
    else:
        logger.inform(s = "Please specify a bucket name.")