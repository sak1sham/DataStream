from pymongo import MongoClient
import certifi
import datetime
import pytz

client = MongoClient('mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservicev2.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority', tlsCAFile=certifi.where())
database_ = client['support-service']
coll = database_['support_tickets_rating']

start = pytz.utc.localize(datetime.datetime(2022, 2, 22))
end = pytz.utc.localize(datetime.datetime(2022, 2, 23))

curs = coll.find({'createdAt': {'$gte': start, '$lt': end}})
ls = list(curs)
print(len(ls))