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
        if(db['destination_type'] == 'local'):
            if not exists("./converted/" + db['source_type']):
                makedirs("./converted/" + db['source_type'])
            if not exists("./converted/" + db['source_type'] + "/" + db['db_name']):
                makedirs("./converted/" + db['source_type'] + "/" + db['db_name'])