from dst.redshift import RedshiftSaver
from dst.s3 import s3Saver
from dst.pgsql import PgSQLSaver
from helper.exceptions import *
from typing import List, Dict, Any

class DMS_exporter:
    def __init__(self, db: Dict[str, Any] = None, uid: str = None, partition: List[str] = None, mirroring: bool = False, table_name: str = None) -> None:
        self.type = db['destination']['destination_type']
        self.source_type = db['source']['source_type']
        if(self.type == 's3'):
            self.saver = s3Saver(db_source = db['source'], db_destination = db['destination'], c_partition = partition, unique_id = uid)
        elif(self.type == 'redshift'):
            if(self.source_type == 'api'):
                bulk_data = db['source']['bulk_data'] if 'bulk_data' in db['source'] else False
                self.saver = RedshiftSaver(db_source = db['source'], db_destination = db['destination'], unique_id = uid, is_small_data = bulk_data)
            else:
                self.saver = RedshiftSaver(db_source = db['source'], db_destination = db['destination'], unique_id = uid)
            if(mirroring):
                self.saver.delete_table(table_name=table_name)
        elif(self.type == 'pgsql'):
            self.saver = PgSQLSaver(db_source = db['source'], db_destination = db['destination'], unique_id = uid)
            if(mirroring):
                self.saver.delete_table(table_name=table_name)
        else:
            raise DestinationNotFound("Destination type not recognized. Choose from s3, redshift, pgsql")

    def get_n_cols(self, table_name: str = None) -> int:
        return self.saver.get_n_cols(table_name=table_name)

    def count_n_records(self, table_name: str = None) -> int:
        return self.saver.count_n_records(table_name=table_name)

    def drop_table(self, table_name: str = None) -> None:
        self.saver.delete_table(table_name=table_name)

    def save(self, processed_data: Dict[str, Any]= None, primary_keys: List[str] = None, c_partition: List[str] = None) -> None:
        if(c_partition and self.type not in ['redshift', 'pgsql']):
            self.saver.save(processed_data = processed_data, c_partition = c_partition, primary_keys = primary_keys)
        else:
            self.saver.save(processed_data = processed_data, primary_keys = primary_keys)

    def expire(self, expiry: Dict[str, int] = None, tz_info: Any = None):
        if(expiry):
            self.saver.expire(expiry, tz_info)

    def close(self):
        self.saver.close()