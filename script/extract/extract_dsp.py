from script.utils.loggingSetup import log_error, log_info, log_warning
from script.utils.create_directory import create_directory
import os
from dotenv import load_dotenv
import csv
import time
import oracledb

#load environment variables and set
load_dotenv()
oracle_db = os.getenv('ORACLE_DB')
oracle_un = os.getenv('ORACLE_UN')
oracle_pw = os.getenv('ORACLE_PW')

#function to get data from oracle db and save to csv, startdate can be set, otherwise default is 2000-01-01 (should be all the data)
def extract_dsp(table_name, entry_date='2000-01-01'):
    try:
        log_info('Extracting data from Oracle DB')


        #saving data path in variable
        data_path = f'data/raw/{table_name}'
        #saving file name in variable (table_name + timestamp.csv)
        file_name = f'{table_name}_{time.strftime("%Y%d%m_%H%M%S")}.csv'
        #saving full path in variable
        full_path = os.path.join(data_path, file_name)
        #table_info path
        table_info_path = f'data/table_info/{table_name}'


        #creating directory for data if not exists
        val = create_directory(data_path)
        #create directory for table_info if not exists
        val = create_directory(table_info_path)

      
        #connect to oracle db
        log_info('Connecting to Oracle DB')
        oracle_conn = oracledb.connect(user=oracle_un, password=oracle_pw, dsn=oracle_db)
        oracle_cursor = oracle_conn.cursor()
        log_info(f'Successfully connected to Oracle DB {oracle_db}')


        #get data from oracle db where A_DATE >= entry_date
        oracle_cursor.execute(f'SELECT * FROM DSPJTENERGY.{table_name} WHERE A_DATE >= TO_DATE(\'{entry_date}\', \'YYYY-MM-DD\')')
        data = oracle_cursor.fetchall()
        data_columns = oracle_cursor.description


        #get table info from oracle db
        oracle_cursor.execute(f"SELECT table_name,column_name,data_type, data_length, data_precision FROM all_tab_cols WHERE TABLE_NAME = '{table_name}' AND OWNER = 'DSPJTENERGY' AND HIDDEN_COLUMN = 'NO'")
        table_info = oracle_cursor.fetchall()
        table_info_columns = oracle_cursor.description


        #save data to csv in raw folder
        with open(full_path, 'w+', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            #create array of header names
            headers = []
            for column in data_columns:
                headers.append(column[0])
            # Write the header
            writer.writerow(headers)
            # Write the rows
            writer.writerows(data)


        #save table info to csv in raw folder
        with open(f'{table_info_path}/{table_name}_table_info.csv', 'w+', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            #create array of header names
            headers = []
            for column in table_info_columns:
                headers.append(column[0])
            # Write the header
            writer.writerow(headers)
            # Write the rows
            writer.writerows(table_info)

        log_info('Data successfully extracted from Oracle DB and saved to csv')
        return full_path
    except Exception as e:
        log_error(f'Error extracting data from Oracle DB: {e}')
        raise e
    