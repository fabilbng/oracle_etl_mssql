from script.utils.setup_logging import setup_logging
from script.oracle_pipeline import OraclePipeline
from script.utils.storage_cleanup import storage_cleanup
import logging
import json
import sys




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
            logger.info(f'Running pipeline for table {table}')

            exclude_columns = settings['tables'][tables.index(table)]['exclude_columns']
            pipeline.run_pipeline(table_name=table, exclude_columns=exclude_columns)

        except Exception as e:
            logger.error(f'Error running pipeline for table {table}: {e}')



if __name__ == '__main__':
    setup_logging()
    run_pipelines()
    storage_cleanup()