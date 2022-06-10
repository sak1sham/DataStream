from pymongo import MongoClient
import awswrangler as wr
import random
import datetime
from typing import NewType
from bson import ObjectId
from dotenv import load_dotenv
import sys
import time 
import traceback
from typing import Dict, Any
load_dotenv()
import pytz
datetype = NewType("datetype", datetime.datetime)
import os
from dotenv import load_dotenv
load_dotenv()
import json

from migration_mapping import get_mapping
from test_util import *
from slack_notify import send_message
from testing_logger import logger

def convert_to_str(x) -> str:
    if(isinstance(x, list) or isinstance(x, dict)):
        return json.dumps(x)
    elif(isinstance(x, datetime.datetime)):
        x = convert_to_datetime(x)
        return x.strftime("%Y/%m/%dT%H:%M:%S")
    else:
        return str(x)

certificate = 'config/rds-combined-ca-bundle.pem'

class MongoTester():
    def __init__(self, id_: str = '', url: str = '', db: Dict = {}, col: Dict = {}, test_N: int = 1000, col_map: Dict = {}, primary_key: str = '', tz_info: Any = pytz.timezone("Asia/Kolkata")):
        self.id_ = id_
        self.url = url
        self.db = db
        self.col = col
        self.test_N = test_N
        self.col_map = col_map
        self.primary_key = primary_key
        self.tz_info = tz_info
        self.N_mongo = -1
        self.count = 0
    
    def get_last_run_cron_job(self):
        client_encr = MongoClient(os.getenv('ENCR_MONGO_URL'), tlsCAFile=certificate)
        db_encr = client_encr[os.getenv('DB_NAME')]
        collection_encr = db_encr[os.getenv('COLLECTION_NAME')]
        curs = collection_encr.find({'last_run_cron_job_for_id': self.id_})
        curs = list(curs)
        return curs[0]['timing']

    def count_docs(self) -> int:
        if(self.N_mongo == -1):
            client = MongoClient(self.url, tlsCAFile=certificate)
            db = client[self.db]
            collection = db[self.col]
            self.N_mongo = collection.count_documents({})
        return self.N_mongo
    
    def check_match(self, record, athena_record) -> bool:
        try:
            for key in record.keys():
                if(record[key]):
                    try:
                        athena_key = key.lower()
                        if(key == '_id'):
                            assert str(record[key]) == athena_record[athena_key]
                        elif(key in self.col_map['fields'].keys()):
                            val = self.col_map['fields'][key]
                            if record[key]:
                                if(val == 'int'):
                                    assert int(float(record[key])) == athena_record[athena_key]
                                elif(val == 'float'):
                                    assert float(record[key]) == athena_record[key]
                                elif(val == 'bool'):
                                    record[key] = str(record[key])
                                    assert (record[key].lower() in ['true', '1', 't', 'y', 'yes'] and athena_record[athena_key]) or (record[key].lower() not in ['true', '1', 't', 'y', 'yes'] and not athena_record[athena_key])
                                elif(val == 'datetime'):
                                    date1 = convert_to_datetime(record[key])
                                    date2 = convert_to_datetime(athena_record[athena_key])
                                    assert (date1 is pd.NaT and date2 is pd.NaT) or (abs((date1-date2).total_seconds()) <= 1)
                                else:
                                    assert convert_to_str(record[key]) == athena_record[athena_key]
                        else:
                            assert convert_to_str(record[key]) == athena_record[athena_key]
                    except Exception as e:
                        logger.err(key)
                        logger.err(str(record['_id']))
                        logger.err('Source: ' + str(record[key]))
                        logger.err('Destination: ' + str(athena_record[key.lower()]))
                        raise
            return True
        except Exception as e:
            logger.err(str(e))

    def test_mongo(self):
        client = MongoClient(self.url, tlsCAFile=certificate)
        db = client[self.db]
        collection = db[self.col]
        N = self.count_docs()
        prev_time = pytz.utc.localize(self.get_last_run_cron_job())
        logger.inform("Previous run cron time: " + str(prev_time))
        compare_datetime = prev_time
        if('buffer_updation_lag' in self.col_map.keys() and self.col_map['buffer_updation_lag']):
            days = 0
            hours = 0
            minutes = 0
            if('days' in self.col_map['buffer_updation_lag'].keys() and self.col_map['buffer_updation_lag']['days']):
                days = self.col_map['buffer_updation_lag']['days']
            if('hours' in self.col_map['buffer_updation_lag'].keys() and self.col_map['buffer_updation_lag']['hours']):
                hours = self.col_map['buffer_updation_lag']['hours']
            if('minutes' in self.col_map['buffer_updation_lag'].keys() and self.col_map['buffer_updation_lag']['minutes']):
                minutes = self.col_map['buffer_updation_lag']['minutes']
            compare_datetime = compare_datetime - datetime.timedelta(days=days, hours=hours, minutes=minutes)
        logger.inform("Testing records before: " + str(compare_datetime))
        last_run_cron_job_id = ObjectId.from_datetime(prev_time)
        query = {
            "_id": {
                "$lte": last_run_cron_job_id,
            }
        }
        skipped_docs = random.randint(0, N)
        logger.inform(f"Skipping {skipped_docs} documents before fetching {self.test_N} continuous docs")
        curs = collection.find(query).limit(self.test_N).skip(skipped_docs)
        curs = list(curs)
        
        str_id = ""
        for records in curs:
            str_id += "\'" + str(records['_id']) + "\',"

        n_recs = len(curs)
        if(n_recs):
            query = 'SELECT * FROM ' + self.col + ' WHERE _id in (' + str_id[:-1] + ');'
            database = "mongo" + "_" + self.db.replace('.', '_').replace('-', '_')
            df = wr.athena.read_sql_query(pgsql = query, database = database)
            for record in curs:
                if('bookmark' in self.col_map.keys() and self.col_map['bookmark']):
                    if('improper_bookmarks' in self.col_map.keys() and not self.col_map['improper_bookmarks']):
                        if(pytz.utc.localize(record[self.col_map['bookmark']]) > compare_datetime):
                            n_recs -= 1
                            logger.inform('Record updated later.')
                            continue
                    else:
                        if(convert_to_datetime(record[self.col_map['bookmark']], pytz.utc) > compare_datetime):
                            n_recs -= 1
                            logger.inform("Record updated later.")
                            continue
                athena_record = df.loc[df['_id'] == str(record['_id'])].to_dict(orient='records')
                try:
                    assert self.check_match(record, athena_record[0])
                except Exception as e:
                    logger.err(traceback.format_exc())
                    logger.err(record['_id'])
                    logger.err("Assertion Error found.")
                    self.count += 1
        logger.inform("Tested {0} records.".format(n_recs))

if __name__ == "__main__":
    try:
        n_test = 1000
        records_per_batch = 1000
        id = ''
        if(len(sys.argv) > 1):
            id = sys.argv.pop()
        mapping = get_mapping(id)
        if('collections' not in mapping.keys()):
            mapping['collections'] = []
        for col in mapping['collections']:
            start = time.time()
            obj = MongoTester(url=mapping['source']['url'], db = mapping['source']['db_name'], id_ = id + "_DMS_" + col['collection_name'], col = col['collection_name'], col_map = col, primary_key = '_id', test_N=records_per_batch)
            logger.inform("Testing " +  str(col['collection_name']))
            max_checks = max(1, (obj.count_docs())//records_per_batch)
            for iter in range(0, min(n_test, max_checks)):
                logger.inform("Iteration: " + str(iter))
                obj.test_mongo()
            mismatch = obj.count
            end = time.time()
            time_taken = str(datetime.timedelta(seconds=int(end-start)))
            if('notify' in settings.keys() and settings['notify']):
                msg = "Testing completed for *{0}* from database *{1}* ({2}) with desination {3}.\nTested {4} random records\nTotal time taken {5}\nFound {6} mismatches".format(col['collection_name'], mapping['source']['db_name'], mapping['source']['source_type'], mapping['destination']['destination_type'], n_test*records_per_batch, str(time_taken), mismatch)
                if(mismatch > 0):
                    msg = msg + " <!channel>"
                try:
                    slack_token = settings['slack_notif']['slack_token']
                    channel = mapping['slack_channel'] if 'slack_channel' in mapping and mapping['slack_channel'] else settings['slack_notif']['channel']
                    send_message(msg = msg, channel = channel, slack_token = slack_token)
                    logger.inform("Testing notification sent successfully.")
                except Exception as e:
                    logger.err(traceback.format_exc())
                    logger.err("Unable to connect to slack and send the notification.")
    except Exception as e:
        if('notify' in settings.keys() and settings['notify']):
            msg = "Testing failed for *{0}* from database *{1}* ({2}) with desination {3} with following exception:\n```{4}```".format(col['collection_name'], mapping['source']['db_name'], mapping['source']['source_type'], mapping['destination']['destination_type'], traceback.format_exc())
            try:
                slack_token = settings['slack_notif']['slack_token']
                channel = mapping['slack_channel'] if 'slack_channel' in mapping and mapping['slack_channel'] else settings['slack_notif']['channel']
                send_message(msg = msg, channel = channel, slack_token = slack_token)
                logger.inform("Testing notification sent successfully.")
            except Exception as e:
                logger.err(traceback.format_exc())
                logger.err("Unable to connect to slack and send the notification.")
    