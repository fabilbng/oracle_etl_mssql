from script.utils.loggingSetup import log_error, log_info, log_warning
import os
from dotenv import load_dotenv
import sqlalchemy
import pandas as pd

#load environment variables and set mssql-variables
load_dotenv()
mssql_dbs = os.getenv('MSSQL_DBS')
mssql_db = os.getenv('MSSQL_DB')
mssql_un = os.getenv('MSSQL_UN')
mssql_pw = os.getenv('MSSQL_PW')
env = os.getenv('ENV')



def load_to_mssql_variable_columns(transformed_path, table_name):

    #creat sqlalchemy engine, using sqlalchemy since it is easier to load data to mssql table
    if env == 'dev':
        engine = sqlalchemy.create_engine(f'mssql+pyodbc://{mssql_dbs}/{mssql_db}?driver=SQL+Server')
    elif env == 'prod':
        engine = sqlalchemy.create_engine(f'mssql+pyodbc://{mssql_un}:{mssql_pw}@{mssql_dbs}/{mssql_db}?driver=SQL+Server')
    #loading data to mssql table, table name must be given, columns are variable depending on csv format
    try:
        log_info('Loading data to MSSQL DB')

        #read csv with headers
        df = pd.read_csv(transformed_path, header=0, encoding='utf-8', engine='python')
        




        log_info('Data successfully loaded to MSSQL DB')
    except Exception as e:
        log_error(f'Error loading data to MSSQL DB: {e}')
        raise e