
from script.oracle_pipeline import OraclePipeline
import logging
import datetime



def main():
    # Set up the logger
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)  # Set the minimum logging level

    # Create a formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create a file handler
    timestamp = datetime.datetime.now().strftime('%Y-%d-%m_%H-%M-%S')
    file_handler = logging.FileHandler(f'logs/logfile_{timestamp}.log')
    file_handler.setLevel(logging.DEBUG)  # Set the desired level for the file handler
    file_handler.setFormatter(formatter)

    # Create a stream handler (for command line output)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)  # Set the desired level for the stream handler
    stream_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)



    logger.info('Starting script..')
    Pipeline = OraclePipeline()
    Pipeline.run_pipeline('BESDAT')


main()