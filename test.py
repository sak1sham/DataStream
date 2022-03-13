from time import sleep
from typing import Tuple
from pymongo import MongoClient
import certifi
import datetime
import pytz
from fastapi import FastAPI
import uvicorn

# client = MongoClient('mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservicev2.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority', tlsCAFile=certifi.where())
# database_ = client['support-service']
# coll = database_['support_tickets']

# start = pytz.utc.localize(datetime.datetime(2022, 3, 4))
# end = pytz.utc.localize(datetime.datetime(2022, 3, 5))

# curs = coll.find({'created_at': {'$gte': start, '$lt': end}})
# ls = list(curs)
# print(len(ls))

print(1)
# while True:
#     print(1)
#     sleep(1000)

from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}

uvicorn.run(app, port=3000)