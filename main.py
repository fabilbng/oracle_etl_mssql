from script.utils.setup_logging import setup_logging
from script.oracle_pipeline import OraclePipeline
from script.utils.storage_cleanup import storage_cleanup
import logging
import json
import sys
import datetime
from script.utils.create_directory import create_directory



#function to run pipelines
def run_pipelines():
    logger = logging.getLogger(__name__)
    try:
        #read settings json from root directory
        with open('settings.json') as json_file:
            settings = json.load(json_file)
    except Exception as e:
        logger.error(f'Error reading settings.json: {e}')
        sys.exit('Error reading settings.json')


    #get array of tables from settings, settings has array of dictionaries with table "name" and "exclude_columns"
    tables = [table['name'] for table in settings['tables']]
    logger.debug(f'Tables to run: {tables}')

    pipeline = OraclePipeline()
    for table in tables:
        try:
            exclude_columns = settings['tables'][tables.index(table)]['exclude_columns']
            pipeline.run_pipeline(table_name=table, exclude_columns=exclude_columns)

        except Exception as e:
            logger.error(f'Pipeline for {table} stopped: {e}')

#function to setup logging
def setup_logging():
    try:
            # Set up the logger
        logger = logging.getLogger('')
        logger.setLevel(logging.DEBUG)  # Set the minimum logging level

        # Create a formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Create a file handler
        create_directory('logs')
        timestamp = datetime.datetime.now().strftime('%Y-%d-%m_%H-%M-%S')
        file_handler_normal = logging.FileHandler(f'logs/logfile_{timestamp}_normal.log')
        file_handler_normal.setLevel(logging.INFO)  # Set the desired level for the file handler
        file_handler_normal.setFormatter(formatter)
        file_handler_detailed = logging.FileHandler(f'logs/logfile_{timestamp}_detailed.log')
        file_handler_detailed.setLevel(logging.DEBUG)  # Set the desired level for the file handler
        file_handler_detailed.setFormatter(formatter)

        # Create a stream handler (for command line output)
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)  # Set the desired level for the stream handler
        stream_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(file_handler_normal)
        logger.addHandler(stream_handler)
        logger.addHandler(file_handler_detailed)
    except Exception as e:
        print(f'Error setting up logging: {e}')
        sys.exit('Error setting up logging')




if __name__ == '__main__':
    setup_logging()
    run_pipelines()
    storage_cleanup()