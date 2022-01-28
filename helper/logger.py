import logging

logging.basicConfig(filename="production.log",format='%(asctime)s %(message)s',filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
