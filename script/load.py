from script.utils.loggingSetup import log_error, log_info, log_warning
import os
from dotenv import load_dotenv
import pyodbc
import pandas as pd
import pyodbc
import csv
import time

#load environment variables and set mssql-variables
load_dotenv()
mssql_dbs = os.getenv('MSSQL_DBS')
mssql_db = os.getenv('MSSQL_DB')
mssql_un = os.getenv('MSSQL_UN')
mssql_pw = os.getenv('MSSQL_PW')
env = os.getenv('ENV')



def load_to_mssql_variable_columns(transformed_path, table_name):
    #loading data to mssql table, table name must be given, columns are variable depending on csv format
    try:
        log_info('Loading data to MSSQL DB')
        #get current time for csv name
        current_time = time.strftime("%Y%d%m_%H%M%S")
        #connect to mssql db without MSSQLConnector class
        if env == 'dev':
            mssql_db_conn = pyodbc.connect(f'DRIVER=SQL Server;SERVER={mssql_dbs};DATABASE={mssql_db};trusted=1')
        elif env == 'prod':
            mssql_db_conn = pyodbc.connect(f'DRIVER=SQL Server;SERVER={mssql_dbs};DATABASE={mssql_db};UID={mssql_un};PWD={mssql_pw}')
        else:
            raise ValueError('Wrong value given for env (either dev or prod)')
        
        mssql_cursor = mssql_db_conn.cursor()

        #read csv to pandas dataframe
        df = pd.read_csv(transformed_path, sep=',', encoding='utf-8', engine='python')
        #create directory in loaded folder with table_name if it does not exist
        if not os.path.exists(f'data/loaded/{table_name}'):
            os.makedirs(f'data/loaded/{table_name}')

        
        #load data to mssql table
        #TODO: try using pandas to_sql function, rn not working due to precision error
        for index, row in df.iterrows():
            #get string of values from row, if value is nan, replace with None
            #TODO: find alternative way to load data to mssql table
            string = ''
            for value in row:
                if pd.isna(value):
                    string += 'NULL, '
                else:
                    string += f"'{value}', "
            string = string[:-2]

            #check if row already exists in table, POSNR is primary Key
            mssql_cursor.execute(f"SELECT * FROM {table_name} WHERE POSNR = '{row['POSNR']}'")
            if mssql_cursor.fetchone() is not None:
                #if row exists, update row
                log_info(f'Row with POSNR {row["POSNR"]} already exists..')
            else:
                try:

                    #if row does not exist, insert row
                    log_info(f'Row with POSNR {row["POSNR"]} does not exist, inserting')
                    prepared_statement = f"INSERT INTO {table_name} VALUES ({string})"
                    mssql_cursor.execute(prepared_statement)
                    #commit changes to mssql db
                    mssql_db_conn.commit()

                    #save row to csv in loaded folder, csv name with timestamp
                    with open(f'data/loaded/{table_name}/{table_name}_{current_time}.csv', 'a+', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(row)


                except pyodbc.Error as e:
                    log_error(f'Error loading data to MSSQL DB: {e}')
                    #if insert fails, save row to csv in failed_inserts folder
                    #create failed_inserts folder in table_name_folder if it does not exist
                    if not os.path.exists(f'data/loaded/{table_name}/failed_inserts'):
                        os.makedirs(f'data/loaded/{table_name}/failed_inserts')
                    #save failed row to csv in failed_inserts folder
                    with open(f'data/loaded/{table_name}/failed_inserts/{table_name}_{current_time}_failed_inserts.csv', 'a+', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(row)


        #close connection
        mssql_db_conn.close()
        log_info('Data successfully loaded to MSSQL DB')
    except Exception as e:
        log_error(f'Error loading data to MSSQL DB: {e}')
        raise e