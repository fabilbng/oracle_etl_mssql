import os
from script.utils.loggingSetup import log_error

def create_directory(directory):
    try:
        #checking if directory with table_name exists
        if not os.path.exists(directory):
            os.makedirs(directory)
            return 0 
        return 1
    except Exception as e:
        log_error(f'Error checking for directory: {e}')
        raise e
