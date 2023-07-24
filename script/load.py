from script.utils.loggingSetup import log_error, log_info, log_warning
import os
from dotenv import load_dotenv
from script.utils.baseClasses import MSSQLConnector
import pandas as pd

#load environment variables and set mssql-variables
load_dotenv()
mssql_dbs = os.getenv('MSSQL_DBS')
mssql_db = os.getenv('MSSQL_DB')
mssql_un = os.getenv('MSSQL_UN')
mssql_pw = os.getenv('MSSQL_PW')
env = os.getenv('ENV')



def load_to_mssql_variable_columns(transformed_path, table_name):
    """
    #creat sqlalchemy engine, using sqlalchemy since it is easier to load data to mssql table
    if env == 'dev':
        engine = sqlalchemy.create_engine(f'mssql+pyodbc://{mssql_dbs}/{mssql_db}?driver=SQL+Server')
    elif env == 'prod':
        engine = sqlalchemy.create_engine(f'mssql+pyodbc://{mssql_un}:{mssql_pw}@{mssql_dbs}/{mssql_db}?driver=SQL+Server')
    """
    
    #loading data to mssql table, table name must be given, columns are variable depending on csv format
    try:
        log_info('Loading data to MSSQL DB')

        #read csv to pandas dataframe
        df = pd.read_csv(transformed_path, sep=',', encoding='utf-8', engine='python')

        #connect to mssql db
        if env == 'dev':
            mssql_db_conn = MSSQLConnector(mssql_dbs, mssql_db, mssql_un, mssql_pw, trusted=1)
        elif env == 'prod':
            mssql_db_conn = MSSQLConnector(mssql_dbs, mssql_db, mssql_un, mssql_pw, trusted=0)
        else:
            raise ValueError('Wrong value given for env (either dev or prod)')
        
        mssql_cursor = mssql_db_conn.mssql_cursor



        #check if table exists, if table exists, check if columns match csv columns
        mssql_cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
        if mssql_cursor.fetchone() is not None:
            mssql_cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
            columns = mssql_cursor.fetchall()
            columns = [column[3] for column in columns]
            if columns != list(df.columns):
                log_warning(f'Columns in csv do not match columns in table {table_name}')
                #alter table to match csv columns
                for column in df.columns:
                    if column not in columns:
                        #TODO: find a way to get correct data type from pandas dataframe
                        mssql_cursor.execute(f"ALTER TABLE {table_name} ADD {column} VARCHAR(255)")
                        log_info(f'Added column {column} to table {table_name}')
            else:
                log_info(f'Columns in csv match columns in table {table_name}')
        else:
            #create table with columns from csv
            #TODO: imlement function to create table with correct data types
            log_warning(f'Table {table_name} does not exist')
            #temporary exception 
            raise Exception('Table {table_name} does not exist')
        
        #load data to mssql table
        #TODO: try using pandas to_sql function, rn not working due to precision error
        for index, row in df.iterrows():
            mssql_cursor.execute(f"INSERT INTO {table_name} VALUES ({','.join(['?']*len(row))})", row)
        




        log_info('Data successfully loaded to MSSQL DB')
    except Exception as e:
        log_error(f'Error loading data to MSSQL DB: {e}')
        raise e