from pymongo import MongoClient
import os
from dotenv import load_dotenv
import certifi
load_dotenv()

mongopwd = os.getenv('MONGOPWD')
mongouser = os.getenv('MONGOUSR')
mongourl = 'mongodb+srv://{}:{}@supportservice.3md7h.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'.format(mongouser, mongopwd)

client = MongoClient(mongourl, tlsCAFile=certifi.where())
