import os
import logging

# Create a logger
logger = logging.getLogger(__name__)

def create_directory(directory):
    try:
        #checking if directory with table_name exists
        if not os.path.exists(directory):
            os.makedirs(directory)
            return 1 
        return 0
    except Exception as e:
        logger.error(f'Error checking for directory: {e}')
        raise e
