from distutils.log import Log
import logging

class Log_manager:
    def __init__(self) -> None:
        logging.getLogger().setLevel(logging.INFO)
    def inform(self, s: str) -> None:
        logging.info(s)
    def warn(self, s: str) -> None:
        logging.warning(s)
    def err(self, s: str) -> None:
        logging.error(s)

logger = Log_manager()