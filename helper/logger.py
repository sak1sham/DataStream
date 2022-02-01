import logging
from storage.local import setup_logger_file

setup_logger_file()
logging.basicConfig(filename="logs/production.log",format='%(asctime)s %(message)s',filemode='w+')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info('Created logger directories and started logger')