import awswrangler as wr

import logging
logging.getLogger().setLevel(logging.INFO)

from typing import List, Dict, Any

class s3Saver:
    def __init__(self, db_source: Dict[str, Any] = {}, db_destination: Dict[str, Any] = {}, c_partition: List[str] = [], unique_id: str = "") -> None:
        self.s3_location = "s3://" + db_destination['s3_bucket_name'] + "/" + db_source['source_type'] + "/" + db_source['db_name'] + "/"
        self.partition_cols = c_partition
        self.unique_id = unique_id

    def inform(self, message: str = "") -> None:
        logging.info(self.unique_id + ": " + message)
    
    def warn(self, message: str = "") -> None:
        logging.warning(self.unique_id + ": " + message)

    def save(self, processed_data: Dict[str, Any] = {}, c_partition: List[str] = []) -> None:
        if(len(c_partition) > 0):
            self.partition_cols = c_partition
        file_name = self.s3_location + processed_data['name'] + "/"
        self.inform("Attempting to insert " + str(processed_data['df_insert'].memory_usage(index=True).sum()) + " bytes.")
        print(dict(processed_data['df_insert'].dtypes))
        if(processed_data['df_insert'].shape[0] > 0):
            wr.s3.to_parquet(
                df = processed_data['df_insert'],
                path = file_name,
                compression='snappy',
                dataset = True,
                partition_cols = self.partition_cols,
            )
        self.inform("Inserted " + str(processed_data['df_insert'].shape[0]) + " records.")

        self.inform("Attempting to update " + str(processed_data['df_update'].memory_usage(index=True).sum()) + " bytes.")    
        file_name_u = self.s3_location + processed_data['name'] + "/"
        for i in range(processed_data['df_update'].shape[0]):
            for x in self.partition_cols:
                file_name_u = file_name_u + x + "=" + str(processed_data['df_update'].iloc[i][x]) + "/"
            df_to_be_updated = wr.s3.read_parquet(
                path = file_name_u,
                dataset = True
            )
            df_to_be_updated[df_to_be_updated['_id'] == processed_data['df_update'].iloc[i]['_id']] = processed_data['df_update'].iloc[i]
            wr.s3.to_parquet(
                df = df_to_be_updated,
                mode = 'overwrite',
                path = file_name_u,
                compression = 'snappy',
                dataset = True,
            )
        self.inform(str(processed_data['df_update'].shape[0]) + " updations done.")
