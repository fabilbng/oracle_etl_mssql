
import os
import pyodbc
from dotenv import load_dotenv
from datetime import datetime
from script.utils.loggingSetup import log_info, log_error, log_warning

#load environment variables and set mssql-variables
load_dotenv()
mssql_dbs = os.getenv('MSSQL_DBS')
mssql_db = os.getenv('MSSQL_DB')
mssql_un = os.getenv('MSSQL_UN')
mssql_pw = os.getenv('MSSQL_PW')
env = os.getenv('ENV')

connect_string_2  = f'DRIVER=SQL Server;SERVER={mssql_dbs};DATABASE={mssql_db};UID={mssql_un};PWD={mssql_pw}'
connect_string_1 = f'DRIVER=SQL Server;SERVER={mssql_dbs};DATABASE={mssql_db};trusted=1'

#function that connects to mssql db and selects the last update date from the table LastUpdate and returns it
def get_last_update_date(table_name):
    try:
        log_info('Getting last update date from table LastUpdate')
        #connect to mssql db without MSSQLConnector class
        if env == 'dev':
            mssql_db_conn = pyodbc.connect(connect_string_1)
        elif env == 'prod':
            mssql_db_conn = pyodbc.connect(connect_string_2)
        else:
            raise ValueError('Wrong value given for env (either dev or prod)')
        
        mssql_cursor = mssql_db_conn.cursor()

        #get last update date from table LastUpdate
        mssql_cursor.execute(f"SELECT * FROM LastUpdate WHERE TableName = '{table_name}'")
        last_update_date = mssql_cursor.fetchone()[1]
        #convert last update date to datetime object
        mssql_db_conn.close()
        return last_update_date
    except Exception as e:
        log_error(f'Error getting last update date from table LastUpdate: {e}')
        raise e

#function that connects to mssql db and updates the last update date in the table LastUpdate
def set_last_update_date(table_name):
    try:
        log_info('Setting last update date in table LastUpdate')
        #connect to mssql db without MSSQLConnector class
        if env == 'dev':
            mssql_db_conn = pyodbc.connect(connect_string_1)
        elif env == 'prod':
            mssql_db_conn = pyodbc.connect(connect_string_2)
        else:
            raise ValueError('Wrong value given for env (either dev or prod)')
        
        mssql_cursor = mssql_db_conn.cursor()

        #get current time
        current_date = datetime.now().strftime("%Y-%m-%d")
        #update last update date in table LastUpdate
        mssql_cursor.execute(f"UPDATE LastUpdate SET LastUpdate = '{current_date}' WHERE TableName = '{table_name}'")
        mssql_db_conn.commit()
        mssql_db_conn.close()
    except Exception as e:
        log_error(f'Error setting last update date in table LastUpdate: {e}')
        raise e
