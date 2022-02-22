from dst.redshift import RedshiftSaver
from dst.s3 import s3Saver
from helper.exceptions import *
from typing import List, Dict, Any

class DMS_exporter:
    def __init__(self, db: Dict[str, Any] = None, uid: str = None, partition: List[str] = None) -> None:
        if(db['destination']['destination_type'] == 's3'):
            self.saver = s3Saver(db_source = db['source'], db_destination = db['destination'], c_partition = partition, unique_id = uid)
        elif(self.db['destination']['destination_type'] == 'redshift'): 
            self.saver = RedshiftSaver(db_source = db['source'], db_destination = db['destination'], unique_id = uid)
        else:
            raise DestinationNotFound("Destination type not recognized. Choose from s3, redshift")

    def save(self, processed_data: Dict[str, Any]= None, primary_keys: List[str] = None, c_partition: List[str] = None) -> None:
        if(primary_keys):
            self.saver.save(processed_data, primary_keys)
        elif(c_partition):
            self.saver.save(processed_data, c_partition)
        else:
            self.saver.save(processed_data)

    def expire(self, expiry: Dict[str, int] = None, tz_info: Any = None):
        if(expiry):
            self.saver.expire(expiry, tz_info)

    def close(self):
        self.saver.close()