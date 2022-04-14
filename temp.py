from pymongo import MongoClient
import datetime
import pytz

client = MongoClient('mongodb://manish:ACVVCH7t7rqd8kB8@supportv2.cbo3ijdmzhje.ap-south-1.docdb.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&retryWrites=false', tlsCAFile = 'config/rds-combined-ca-bundle.pem')
db = client['support-service']
col = db['support_tickets']

query = {
    '$and': [
        {
            'created_at': {
                "$gte" : datetime.datetime(2022, 4, 7, tzinfo = pytz.timezone('Asia/Kolkata')),
                "$lt": datetime.datetime(2022, 4, 8, tzinfo = pytz.timezone('Asia/Kolkata')),
            }
        }, 
        {
            'status': {
                '$in': [4,5]
            }
        }
    ]
}

l = col.count_documents(query)
print(l)