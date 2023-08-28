from script.utils.create_directory import create_directory
import logging
import datetime
import sys
import os

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


