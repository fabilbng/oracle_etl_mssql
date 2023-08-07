from dotenv import load_dotenv
import os
import pandas as pd
import datetime
import oracledb
import pyodbc
from script.utils.loggingSetup import log_error, log_info, log_warning, log_debug
from script.utils.create_directory import create_directory
import csv
import shutil
import numpy as np

load_dotenv()
oracle_db = os.getenv('ORACLE_DB')
oracle_un = os.getenv('ORACLE_UN')
oracle_pw = os.getenv('ORACLE_PW')
mssql_dbs = os.getenv('MSSQL_DBS')
mssql_db = os.getenv('MSSQL_DB')
mssql_un = os.getenv('MSSQL_UN')
mssql_pw = os.getenv('MSSQL_PW')
env = os.getenv('ENV')


#class that takes in a table name, it initiliazes itself by connecting to the mssql db and oracle db 
#it has a method that gets the last update date from the mssql db and a method that sets the last update date in the mssql db
#it has a method that extracts the data from the oracle db and a method that transforms the data
#it has a method that loads the data into the mssql db
class OraclePipeline:
    def __init__(self, table_name='ARTLIF'):
        try:
            log_info(f'Initializing OraclePipeline')
            self.table_name = table_name
            #timestamp of when the pipeline is run
            self.run_date = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            log_info('Connecting to oracle database')
            self.oracle_conn = oracledb.connect(user=oracle_un, password=oracle_pw, dsn=oracle_db)

            log_info('Connecting to mssql database')
            connect_string_2  = f'DRIVER=SQL Server;SERVER={mssql_dbs};DATABASE={mssql_db};UID={mssql_un};PWD={mssql_pw}'
            connect_string_1 = f'DRIVER=SQL Server;SERVER={mssql_dbs};DATABASE={mssql_db};trusted=1'
            if env == 'dev':
                self.mssql_conn = pyodbc.connect(connect_string_1)
            elif env == 'prod':
                self.mssql_conn = pyodbc.connect(connect_string_2)
            else:
                raise ValueError('Wrong value given for env (either dev or prod)')

            #creating neccessary folders
            log_info('Creating neccessary folders')

        except Exception as e:
            log_error(f'Error initializing OraclePipeline: {e}')
            raise e    


    #function that connects to mssql db and gets the last update date from the table LastUpdate
    def get_last_update_date(self):
        try:
            log_info(f'Getting last update date from table LastUpdate for table {self.table_name}')
            cursor = self.mssql_conn.cursor()
            #get last update date from table LastUpdate
            cursor.execute(f"SELECT * FROM LastUpdate WHERE TableName = '{self.table_name}'")
            last_update_date = cursor.fetchone()[2]
            #convert last update date to datetime object
            return last_update_date
        except Exception as e:
            log_error(f'Error getting last update date from table LastUpdate: {e}')
            raise e



    #function that connects to mssql db and updates the last update date in the table LastUpdate
    def set_last_update_date(self):
        try:
            mssql_cursor = self.mssql_conn.cursor()
            #get current time
            current_date = datetime.now().strftime("%Y-%m-%d")
            #update last update date in table LastUpdate
            mssql_cursor.execute(f"UPDATE LastUpdate SET LastUpdate = '{current_date}' WHERE TableName = '{self.table_name}'")
            self.mssql_conn.commit()
        except Exception as e:
            log_error(f'Error setting last update date in table LastUpdate: {e}')
            raise e




    #function that gets the the data from the oracle db and table info from oracle
    def extract(self):
        try:
            cursor = self.oracle_conn.cursor()
            entry_date = self.get_last_update_date()
            #get data from oracle db where A_DATE >= entry_date
            log_info(f'Getting data from oracle db where A_DATE >= {entry_date}')
            cursor.execute(f'SELECT * FROM DSPJTENERGY.{self.table_name} WHERE A_DATE >= TO_DATE(\'{entry_date}\', \'YYYY-MM-DD\')')
            data = cursor.fetchall()
            data_columns = cursor.description

            #get table info from oracle
            #get table info from oracle db
            log_info(f'Getting table info from oracle db for {self.table_name}')
            cursor.execute(f"SELECT column_name,data_type, data_length, data_scale FROM all_tab_cols WHERE TABLE_NAME = '{self.table_name}' AND OWNER = 'DSPJTENERGY' AND HIDDEN_COLUMN = 'NO'")
            table_info = cursor.fetchall()
            table_info_columns = cursor.description


            #create raw folder if it doesn't exist
            raw_path = f'data/raw/{self.table_name}'
            create_directory(raw_path)
            #raw path file: table name + timestamp + .csv
            raw_path_file = f'{raw_path}/{self.table_name}_{self.run_date}.csv'

            
            #save data to csv in raw folder
            
            log_info(f'Saving data to csv in raw folder: {raw_path_file}')
            with open(raw_path_file, 'w+', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                #create array of header names
                headers = []
                for column in data_columns:
                    headers.append(column[0])
                # Write the header
                writer.writerow(headers)
                # Write the rows
                writer.writerows(data)


            #create table info folder if it doesn't exist
            table_info_path = f'data/table_info/{self.table_name}'
            create_directory(table_info_path)
            table_info_file = f'{table_info_path}/{self.table_name}_table_info.csv'


            #save table info to csv in raw folder
            log_info(f'Saving table info to csv in table info folder: {table_info_file}')
            with open(table_info_file, 'w+', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                #create array of header names

                
                headers = []
                for column in table_info_columns:
                    headers.append(column[0])
                # Write the header
                writer.writerow(headers)
                # Write the rows
                writer.writerows(table_info)

            return raw_path_file
        except Exception as e:
            log_error(f'Error extracting data from oracle database for table {self.table_name}: {e}')
            raise e

    #function that transforms the data
    def transform(self, raw_path):
        try:
            #create transformed folder if it doesn't exist
            transformed_path = f'data/transformed/{self.table_name}'
            create_directory(transformed_path)
            transformed_file_path = f'{transformed_path}/{self.table_name}_{self.run_date}_transformed.csv'
            #log_info(f'Transforming data from {raw_path} and saving it to {transformed_path_file}')
            #tmove data from raw folder to transformed folder
            shutil.move(raw_path, transformed_file_path)
            return transformed_file_path
        except Exception as e:
            log_error(f'Error transforming data for table {self.table_name}: {e}')
            raise e
        
    #function that loads the data to mssql
    def load(self, transformed_file_path):
        #create table in mssql if it doesn't exist
        self.create_table()



    #function that creates a table in mssql based on the table info from oracle, if the table does not already exist, if it exists, it alters it if the structure changed
    def create_table(self):
        #function to create DATA_TYPE_LENGTH_SCALE column from given dataframe
        def create_data_type_length_scale_column(df):
            df['DATA_TYPE_LENGTH_SCALE'] = np.where(df['DATA_TYPE'] == 'VARCHAR', df['DATA_TYPE'] + '(MAX)', np.where(df['DATA_TYPE'] == 'DECIMAL', df['DATA_TYPE'] + '(' + df['DATA_LENGTH'].astype(str) + ',' + df['DATA_SCALE'].astype(str) + ')', df['DATA_TYPE']))
            #remove DATA_TYPE, DATA_LENGTH, DATA_SCALE columns
            df = df.drop(columns=['DATA_TYPE', 'DATA_LENGTH', 'DATA_SCALE'])
            return df

        try:
            #preparing dataframe
            #get table info from csv in table info folder
            oracle_table_info_df = pd.read_csv(f'data/table_info/{self.table_name}/{self.table_name}_table_info_TEST.csv')
            #prepare table info for mssql
            #show table info
            oracle_table_info_df['DATA_TYPE'] = oracle_table_info_df['DATA_TYPE'].str.replace('VARCHAR2', 'VARCHAR')
            #replace number with decimal
            oracle_table_info_df['DATA_TYPE'] = oracle_table_info_df['DATA_TYPE'].str.replace('NUMBER', 'DECIMAL')
            #create new column with data type, length, and precision. If data type is varchar, add length in parenthesis after data type. If data type is number, add length and scale in parenthesis after data type, separated by comma, e.g. VARCHAR(50), NUMBER(10,2), if data type is date, add nothing
            #make data_scale to int (no decimals)
            oracle_table_info_df['DATA_SCALE'] = oracle_table_info_df['DATA_SCALE'].fillna(0).astype(int)
            oracle_table_info_df = create_data_type_length_scale_column(oracle_table_info_df)


            #check if table already exists in mssql
            log_info(f'Checking if table {self.table_name} exists in mssql')
            statement = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{self.table_name}'"
            log_debug(f'Statement: {statement}')
            mssql_cursor = self.mssql_conn.cursor()
            mssql_cursor.execute(statement)
            table_exists = mssql_cursor.fetchone()



            #if table exists, check if table structure is the same
            if table_exists:
                log_info(f'Table {self.table_name} already exists in mssql, checking if strcuture is the same')
                statement = f"SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{self.table_name}'"
                #get table info from mssql
                log_debug(f'Statement: {statement}')
                mssql_cursor.execute(statement)
                mssql_table_info = mssql_cursor.fetchall()
                #convert mssql_table_info to dataframe
                mssql_table_info_df = pd.DataFrame.from_records(mssql_table_info, columns=['COLUMN_NAME', 'DATA_TYPE', 'DATA_LENGTH', 'DATA_SCALE'])
                mssql_table_info_df['DATA_SCALE'] = mssql_table_info_df['DATA_SCALE'].fillna(0).astype(int)
                mssql_table_info_df['DATA_LENGTH'] = mssql_table_info_df['DATA_LENGTH'].fillna(0).astype(int)
                mssql_table_info_df['DATA_TYPE'] = mssql_table_info_df['DATA_TYPE'].str.upper()
                mssql_table_info_df = create_data_type_length_scale_column(mssql_table_info_df)
                #check if table info is the same
                if oracle_table_info_df.equals(mssql_table_info_df):
                    log_info(f'Table {self.table_name} structure is the same, no need to alter table')
                    return 0 #return 0 if table structure is the same
                else:
                    log_info(f'Table {self.table_name} structure is different, altering table')
                    #check which columns are different
                    #get columns that are in oracle but not in mssql
                    oracle_columns_not_in_mssql = oracle_table_info_df[~oracle_table_info_df['COLUMN_NAME'].isin(mssql_table_info_df['COLUMN_NAME'])]
                    #prepare ALTER TABLE statement
                    alter_table_statement = f'ALTER TABLE {self.table_name} '
                    #add columns that are in oracle but not in mssql
                    for index, row in oracle_columns_not_in_mssql.iterrows():
                        alter_table_statement += f'ADD {row["COLUMN_NAME"]} {row["DATA_TYPE_LENGTH_SCALE"]},'
                    #remove last comma
                    alter_table_statement = alter_table_statement[:-1]
                    log_debug(f'Alter table statement: {alter_table_statement}')
                    #execute ALTER TABLE statement
                    mssql_cursor.execute(alter_table_statement)
                    self.mssql_conn.commit()

            else:


                #create table in mssql if does not exist
                log_info(f'Table does not exist creating table {self.table_name} in mssql')
                #create CREATE TABLE statement
                create_table_statement = f'CREATE TABLE {self.table_name} ('
                for index, row in oracle_table_info_df.iterrows():
                    #if column is POSNR, add PRIMARY KEY
                    if row['COLUMN_NAME'] == 'POSNR':
                        create_table_statement += f'{row["COLUMN_NAME"]} {row["DATA_TYPE_LENGTH_SCALE"]} PRIMARY KEY,'
                    else:
                        create_table_statement += f'{row["COLUMN_NAME"]} {row["DATA_TYPE_LENGTH_SCALE"]},'
                #remove last comma
                create_table_statement = create_table_statement[:-1]
                create_table_statement += ')'


                #execute CREATE TABLE statement
                mssql_cursor.execute(create_table_statement)
                self.mssql_conn.commit()

        except Exception as e:
            log_error(f'Error creating table {self.table_name}: {e}')
            raise e



    def run(self):
        raw_path = self.extract()
        transformed_path = self.transform(raw_path)
        self.load(transformed_path)
        self.set_last_update_date()