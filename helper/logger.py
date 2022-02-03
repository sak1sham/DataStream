import logging
from storage.local import setup_logger_file
import os
from dotenv import load_dotenv
load_dotenv()

setup_logger_file()
logging.basicConfig(filename="logs/production.log",format='%(asctime)s %(message)s',filemode='w+')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info('Created logger directories and started logger')

write_mode = os.getenv('RUN_MODE')
if(write_mode is None or not (write_mode == '1' or write_mode=='2')):
    logger.info("Invalid RUN_MODE in .env file. Setting to default 1: print mode")
    write_mode = '2'

def log_writer(x):
    if(write_mode == '1'):
        logger.info(x)
    elif(write_mode == '2'):
        print(x)