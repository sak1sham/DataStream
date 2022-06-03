from datetime import datetime
import logging
from typing import Any
import os

from typing import NewType, Any
datetype = NewType("datetype", datetime)

class Log_manager:
    def __init__(self) -> None:
        p = os.path.abspath(os.getcwd())
        logs_folder = f'{p}/../tmp'
        isExist = os.path.exists(logs_folder)
        if not isExist:
            os.makedirs(logs_folder)
            print("The new directory is created to save logs!")
        logging.basicConfig(
            format = '%(asctime)s %(levelname)-8s %(message)s',
            level = logging.INFO,
            datefmt = '%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler(f'{logs_folder}/debug.log', mode='w'),
                logging.StreamHandler()
            ]
        )
        logging.getLogger().setLevel(logging.INFO)

    def inform(self, s: str = None) -> None:
        logging.info(s)

    def warn(self, s: str = None) -> None:
        logging.warning(s)

    def err(self, s: Any = None) -> None:
        logging.error(s)


logger = Log_manager()