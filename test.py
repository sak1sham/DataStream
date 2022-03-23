print("123)")

# from time import sleep
# from turtle import home
# from typing import Tuple
# from pymongo import MongoClient
# import certifi
# import datetime
# import pytz
# import sys
# from fastapi import FastAPI, Request, APIRouter, Depends
# import uvicorn
# from loguru import logger

# logger.remove()
# logger.add(sys.stdout, colorize=True, format="<green>{time:HH:mm:ss}</green> | {level} | <level>{message}</level>")



# client = MongoClient('mongodb+srv://saksham:xwNTtWtOnTD2wYMM@supportservicev2.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority', tlsCAFile=certifi.where())
# database_ = client['support-service']
# coll = database_['support_tickets']

# start = pytz.utc.localize(datetime.datetime(2022, 3, 4))
# end = pytz.utc.localize(datetime.datetime(2022, 3, 5))

# curs = coll.find({'created_at': {'$gte': start, '$lt': end}})
# ls = list(curs)
# print(len(ls))

# # print(1)
# # while True:
# #     print(1)
# #     sleep(1000)

# from fastapi import FastAPI

# app = FastAPI()

# router = APIRouter()
# @router.get("/ping")
# async def root():
#     return {"message": "pong"}

# @router.get("/api/dms/test/")
# async def root():
#     return {"message": "testback"}

# async def logging_dependency(request: Request):
#     logger.debug(f"{request.method} {request.url}")
#     logger.debug("Params:")
#     for name, value in request.path_params.items():
#         logger.debug(f"\t{name}: {value}")
#     logger.debug("Headers:")
#     for name, value in request.headers.items():
#         logger.debug(f"\t{name}: {value}")

# app.include_router(router, dependencies=[Depends(logging_dependency)])

# uvicorn.run(app, port=3000, host="0.0.0.0")