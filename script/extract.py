from script.utils.loggingSetup import log_error, log_info, log_warning
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

#function to get data from oracle db and save to csv
def extract(table_name, entry_date='2023-01-01'):
    try:
        log_info('Extracting data from Oracle DB')


        #checking if directory with table_name exists
        if not os.path.exists(f'data/raw/{table_name}'):
            os.makedirs(f'data/raw/{table_name}')
        #saving path in variable
        path = f'data/raw/{table_name}'
        #saving file name in variable (table_name + timestamp.csv)
        file_name = f'{table_name}_{time.strftime("%Y%d%m_%H%M%S")}.csv'
        #saving full path in variable
        full_path = os.path.join(path, file_name)


        #connect to oracle db
        log_info('Connecting to Oracle DB')
        oracle_conn = oracledb.connect(user=oracle_un, password=oracle_pw, dsn=oracle_db)
        oracle_cursor = oracle_conn.cursor()
        log_info(f'Successfully connected to Oracle DB {oracle_db}')

        #get data from oracle db where A_DATE >= entry_date
        oracle_cursor.execute(f'SELECT * FROM {table_name} WHERE A_DATE >= TO_DATE(\'{entry_date}\', \'YYYY-MM-DD\')')
        data = oracle_cursor.fetchall()

        #save data to csv in raw folder
        with open(full_path, 'w+', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            #create array of header names
            headers = []
            for column in oracle_cursor.description:
                headers.append(column[0])
            # Write the header
            writer.writerow(headers)
            # Write the rows
            writer.writerows(data)

        log_info('Data successfully extracted from Oracle DB and saved to csv')
        return full_path
    except Exception as e:
        log_error(f'Error extracting data from Oracle DB: {e}')
        raise e
    