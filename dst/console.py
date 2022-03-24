
from helper.logger import logger

from typing import List, Dict, Any
class ConsoleSaver:
    def __init__(self) -> None:
        pass

    def inform(self, message: str = "") -> None:
        logger.inform(self.unique_id + ": " + message)
    
    def warn(self, message: str = "") -> None:
        logger.warn(self.unique_id + ": " + message)

    def save(self, processed_data: Dict[str, Any] = None, primary_keys: List[str] = None) -> None:
        self.inform("Attempting to insert " + str(processed_data['df_insert'].memory_usage(index=True).sum()) + " bytes.")
    
    def expire(self, expiry: Dict[str, int], tz: Any = None) -> None:
        pass
    
    def close(self):
        pass