from os.path import exists
from os import makedirs

def setup_logger_file():
    if not exists("./logs"):
        makedirs("./logs")
    if not exists("./logs/production.log"):
        open("./logs/production.log", "w").close()