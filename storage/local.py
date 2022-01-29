from os.path import exists
from os import makedirs

def create_directories(list_databases):
    '''
        Creates the directories to save converted files
    '''
    if not exists("./logs"):
        makedirs("./logs")
        
    if not exists("./logs/production.log"):
        open("./logs/production.log", "w").close()

    if not exists("./converted"):
        makedirs("./converted")

    for db in list_databases:
        db_name = db['db_name']
        if not exists("./converted/" + db_name):
            makedirs("./converted/" + db_name)