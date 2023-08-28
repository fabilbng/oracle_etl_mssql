from script.utils.create_directory import create_directory
import logging
import datetime
import os



#function to cleanup storage, it deletes all files in the data folder and subdfolders older than 7 days
def storage_cleanup():
    try:
        logger = logging.getLogger(__name__)
        logger.debug('Starting storage cleanup')
        create_directory('data')
        for root, dirs, files in os.walk('data'):
            for file in files:
                file_path = os.path.join(root, file)
                file_creation_time = datetime.datetime.fromtimestamp(os.path.getctime(file_path))
                if file_creation_time < datetime.datetime.now() - datetime.timedelta(days=7):
                    logger.info(f'Deleting file {file_path}')
                    os.remove(file_path)
    except Exception as e:
        logger.error(f'Error cleaning up storage: {e}')
