import logging
from typing import Any

class Log_manager:
    def __init__(self) -> None:
        logging.basicConfig(
            format = '%(asctime)s %(levelname)-8s %(message)s',
            level = logging.INFO,
            datefmt = '%Y-%m-%d %H:%M:%S'
        )
        logging.getLogger().setLevel(logging.INFO)
    def inform(self, s: str) -> None:
        logging.info(s)
    def warn(self, s: str) -> None:
        logging.warning(s)
    def err(self, s: Any) -> None:
        logging.error(s)

logger = Log_manager()