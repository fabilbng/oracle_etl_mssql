import logging
import os
import time

def setup_logger():
    #getting current time for logfile
    current_time = time.localtime()
    formatted_time = time.strftime("%Y%d%m_%H%M%S", current_time)

    #create logs folder if not exists
    logs_folder = "logs"
    if not os.path.exists(logs_folder):
        os.makedirs(logs_folder)

    #create logfile name with the folder path
    logfile_name = os.path.join(logs_folder, f"logfile_{formatted_time}.txt")

    #logging config
    logging.basicConfig(filename=logfile_name, format='%(asctime)s %(levelname)s:%(message)s', encoding='utf-8', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')


def log_info(message):
    print('INFO: ' + message)
    logging.info(message)

def log_error(message):
    print('ERROR: ' + message)
    logging.error(message)

def log_warning(message):
    print('WARNING: ' + message)
    logging.warning(message)

def log_debug(message):
    logging.debug(message)