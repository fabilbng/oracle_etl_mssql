from dotenv import load_dotenv
import os
import pandas as pd
import datetime
import oracledb
import pyodbc
from script.utils.create_directory import create_directory
import csv
import shutil
import numpy as np
import logging

load_dotenv()
oracle_db = os.getenv('ORACLE_DB')
oracle_un = os.getenv('ORACLE_UN')
oracle_pw = os.getenv('ORACLE_PW')
mssql_dbs = os.getenv('MSSQL_DBS')
mssql_db = os.getenv('MSSQL_DB')
mssql_un = os.getenv('MSSQL_UN')
mssql_pw = os.getenv('MSSQL_PW')
env = os.getenv('ENV')
logger = logging.getLogger(__name__)

#class that takes in a table name, it initiliazes itself by connecting to the mssql db and oracle db 
#it has a method that gets the last update date from the mssql db and a method that sets the last update date in the mssql db
#it has a method that extracts the data from the oracle db and a method that transforms the data
#it has a method that loads the data into the mssql db
class OraclePipeline:
    def __init__(self, table_name='ARTLIF'):
        try:
            logger = logging.getLogger(__name__ + '.__init__')
            logger.info(f'Initializing OraclePipeline')
            self.table_name = table_name
            #timestamp of when the pipeline is run
            self.run_date = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            logger.info('Connecting to oracle database')
            self.oracle_conn = oracledb.connect(user=oracle_un, password=oracle_pw, dsn=oracle_db)

            logger.info('Connecting to mssql database')
            connect_string_2  = f'DRIVER=SQL Server;SERVER={mssql_dbs};DATABASE={mssql_db};UID={mssql_un};PWD={mssql_pw}'
            connect_string_1 = f'DRIVER=SQL Server;SERVER={mssql_dbs};DATABASE={mssql_db};trusted=1'
            if env == 'dev':
                self.mssql_conn = pyodbc.connect(connect_string_1)
            elif env == 'prod':
                self.mssql_conn = pyodbc.connect(connect_string_2)
            else:
                raise ValueError('Wrong value given for env (either dev or prod)')

            #creating neccessary folders
            logger.info('Creating neccessary folders')

        except Exception as e:
            logger.error(f'Error initializing OraclePipeline: {e}')
            raise e    


    #function that connects to mssql db and gets the last update date from the table LastUpdate
    def get_last_update_date(self):
        try:
            logger = logging.getLogger(__name__ + '.get_last_update_date')
            logger.info(f'Getting last update date from table LastUpdate for table {self.table_name}')
            cursor = self.mssql_conn.cursor()
            #get last update date from table LastUpdate
            cursor.execute(f"SELECT * FROM LastUpdate WHERE TableName = '{self.table_name}'")
            #check if result is empty
            if cursor.rowcount == 0:
                self.set_last_update_date(new = 1)
                last_update_date = '2000-01-01'
            else:
                last_update_date = cursor.fetchone()[2]
            return last_update_date
        except Exception as e:
            logger.error(f'Error getting last update date from table LastUpdate: {e}')
            raise e



    #function that checks last update on on LastUpdate Table
    def set_last_update_date(self, new = 0):
        try:
            logger = logging.getLogger(__name__ + '.set_last_update_date')
            mssql_cursor = self.mssql_conn.cursor()
            if new:
                logger.info('New table detected')
                logger.info(f'Inserting new row in table LastUpdate for table {self.table_name}')
                #insert new row with current table name and data 2000-01-01
                statement = f"INSERT INTO LastUpdate VALUES('{self.table_name}', '2000-01-01')"
                logger.debug(f'Statement: {statement}')
                mssql_cursor.execute(statement)
                self.mssql_conn.commit()
            else: 
                
                #get current time
                current_date = datetime.datetime.now().strftime("%Y-%m-%d")
                #update last update date in table LastUpdate
                mssql_cursor.execute(f"UPDATE LastUpdate SET Date = '{current_date}' WHERE TableName = '{self.table_name}'")
                self.mssql_conn.commit()
        except Exception as e:
            logger.error(f'Error setting last update date in table LastUpdate: {e}')
            raise e




    #function that gets the the data from the oracle db and table info from oracle
    def extract(self):
        try:
            logger = logging.getLogger(__name__ + '.extract')
            cursor = self.oracle_conn.cursor()
            entry_date = self.get_last_update_date()
            #get data from oracle db where A_DATE >= entry_date
            logger.info(f'Getting data from oracle db where A_DATE >= {entry_date}')
            statement = f'SELECT * FROM DSPJTENERGY.{self.table_name} WHERE A_DATE >= TO_DATE(\'{entry_date}\', \'YYYY-MM-DD\')'
            logger.debug(f'Statement: {statement}')
            cursor.execute(statement)
            data = cursor.fetchall()
            data_columns = cursor.description


            #get table info from oracle db
            logger.info(f'Getting table info from oracle db for {self.table_name}')
            statement = f"SELECT column_name,data_type, data_length, data_scale FROM all_tab_cols WHERE TABLE_NAME = '{self.table_name}' AND OWNER = 'DSPJTENERGY' AND HIDDEN_COLUMN = 'NO'"
            logger.debug(f'Statement: {statement}')
            cursor.execute(statement)
            table_info = cursor.fetchall()
            table_info_columns = cursor.description


            
            #create raw folder if it doesn't exist
            raw_path = f'data/raw/{self.table_name}'
            created = create_directory(raw_path)
            #raw path file: table name + timestamp + .csv
            raw_path_file = f'{raw_path}/{self.table_name}_{self.run_date}.csv'

            
            #save data to csv in raw folder
            
            logger.info(f'Saving data to csv in raw folder: {raw_path_file}')
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
            table_info_file = f'{table_info_path}/{self.table_name}_oracle_table_info.csv'


            #save table info to csv in raw folder
            logger.info(f'Saving table info to csv in table info folder: {table_info_file}')
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
            logger.error(f'Error extracting data from oracle database for table {self.table_name}: {e}')
            raise e

    #function that transforms the data
    def transform(self, raw_path):
        try:
            logger = logging.getLogger(__name__ + '.transform')
            #create transformed folder if it doesn't exist
            transformed_path = f'data/transformed/{self.table_name}'
            create_directory(transformed_path)
            transformed_file_path = f'{transformed_path}/{self.table_name}_{self.run_date}_transformed.csv'
            #logger.info(f'Transforming data from {raw_path} and saving it to {transformed_path_file}')
            #tmove data from raw folder to transformed folder
            shutil.move(raw_path, transformed_file_path)
            return transformed_file_path
        except Exception as e:
            logger.error(f'Error transforming data for table {self.table_name}: {e}')
            raise e
        
    #function that loads the data to mssql
    def load(self, transformed_file_path):
        try:
            logger = logging.getLogger(__name__ + '.load')
            logger.info('Loading data to MSSQL DB')
            #create table in mssql if it doesn't exist
            #val not being used 0 options: 0 = table already exists, 1 = altered, 2 = created
            val = self.create_table()
            mssql_cursor = self.mssql_conn.cursor()

            #read csv to pandas dataframe
            loaded_path = f'data/loaded/{self.table_name}'
            loaded_file_path = f'{loaded_path}/{self.table_name}_{self.run_date}_loaded.csv'
            data_df = pd.read_csv(transformed_file_path, sep=',', encoding='utf-8', engine='python')
            #create directory in loaded folder with table_name if it does not exist, 1 if created, 0 if already exists
            created = create_directory(loaded_path)

            #get headers from csv
            headers = list(data_df.columns.values)
            #prepare headers for sql statement
            headers_string = ''
            for header in headers:
                headers_string += f'{header}, '
            headers_string = headers_string[:-2]
            #prepare statement
            prepared_statement = f"INSERT INTO {self.table_name} ({headers_string}) "
          
            #load data to mssql table
            for index, row in data_df.iterrows():
                #get string of values from row, if value is nan, replace with None
                string = ''
                for value in row:
                    if pd.isna(value):
                        string += 'NULL, '
                    else:
                        string += f"'{value}', "
                string = string[:-2]

                #check if row already exists in table, POSNR is primary Key
                statement = f"SELECT * FROM {self.table_name} WHERE POSNR = '{row['POSNR']}'"
                logger.info(f'Checking if row with POSNR {row["POSNR"]} already exists..')
                logger.debug(f'Statement: {statement}')
                mssql_cursor.execute(statement)

                if mssql_cursor.fetchone() is not None:
                    #if row exists
                    logger.warning(f'Row with POSNR {row["POSNR"]} already exists..')
                else:
                    try:

                        #if row does not exist, insert row
                        logger.info(f'Row with POSNR {row["POSNR"]} does not exist, inserting..')
                        prepared_statement_final = f"{prepared_statement} VALUES ({string})"
                        logger.debug(f'Prepared statement: {prepared_statement_final}')
                        mssql_cursor.execute(prepared_statement_final)
                        #commit changes to mssql db
                        self.mssql_conn.commit()

                        #save row to csv in loaded folder, csv name with timestamp
                        with open(loaded_file_path, 'a+', newline='', encoding='utf-8') as f:
                            writer = csv.writer(f)
                            writer.writerow(row)


                    except pyodbc.Error as e:
                        logger.error(f'Error loading row to MSSQL DB, inserting in failed csv: {e}')
                        #if insert fails, save row to csv in failed_inserts folder
                        #create failed_inserts folder in table_name_folder if it does not exist
                        failed_loaded_path = f'data/loaded/{self.table_name}/failed_inserts'
                        failed_loaded_file_path = f'{failed_loaded_path}/{self.table_name}_{self.run_date}_failed_inserts.csv'
                        created = create_directory(failed_loaded_path)
                        #save failed row to csv in failed_inserts folder
                        with open(failed_loaded_file_path, 'a+', newline='', encoding='utf-8') as f:
                            writer = csv.writer(f)
                            writer.writerow(row)


            logger.info('Data successfully loaded to MSSQL DB')
        except Exception as e:
            logger.error(f'Error loading data to MSSQL DB: {e}')
            raise e




    #function that creates a table in mssql based on the table info from oracle, if the table does not already exist, if it exists, it alters it if the structure changed
    def create_table(self):
        #function to create DATA_TYPE_LENGTH_SCALE column from given dataframe
        def create_data_type_length_scale_column(df):
            df['DATA_TYPE_LENGTH_SCALE'] = np.where(df['DATA_TYPE'] == 'VARCHAR', df['DATA_TYPE'] + '(MAX)', np.where(df['DATA_TYPE'] == 'DECIMAL', df['DATA_TYPE'] + '(' + df['DATA_LENGTH'].astype(str) + ',' + df['DATA_SCALE'].astype(str) + ')', df['DATA_TYPE']))
            #remove DATA_TYPE, DATA_LENGTH, DATA_SCALE columns
            df = df.drop(columns=['DATA_TYPE', 'DATA_LENGTH', 'DATA_SCALE'])
            return df

        try:
            logger = logging.getLogger(__name__ + '.create_table')
            #preparing dataframe
            #get table info from csv in table info folder
            oracle_table_info_df = pd.read_csv(f'data/table_info/{self.table_name}/{self.table_name}_oracle_table_info.csv')
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
            logger.info(f'Checking if table {self.table_name} exists in mssql')
            statement = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{self.table_name}'"
            logger.debug(f'Statement: {statement}')
            mssql_cursor = self.mssql_conn.cursor()
            mssql_cursor.execute(statement)
            table_exists = mssql_cursor.fetchone()



            #if table exists, check if table structure is the same
            if table_exists:
                logger.info(f'Table {self.table_name} already exists in mssql, checking if strcuture is the same')
                statement = f"SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{self.table_name}'"
                #get table info from mssql
                logger.debug(f'Statement: {statement}')
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
                    logger.info(f'Table {self.table_name} structure is the same, no need to alter table')
                    return 0 #return 0 if table structure is the same
                else:
                    logger.info(f'Table {self.table_name} structure is different, altering table')
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
                    logger.debug(f'Alter table statement: {alter_table_statement}')
                    #execute ALTER TABLE statement
                    mssql_cursor.execute(alter_table_statement)
                    self.mssql_conn.commit()
                    return 1 #return 1 if table structure is different

            else:


                #create table in mssql if does not exist
                logger.info(f'Table does not exist creating table {self.table_name} in mssql')
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
                return 2 #return 2 if table is created

        except Exception as e:
            logger.error(f'Error creating table {self.table_name}: {e}')
            raise e


    #running entire pipeline
    def run_pipeline(self, table_name):
        try:
            #setting table name
            self.table_name = table_name
            raw_path = self.extract()
            transformed_path = self.transform(raw_path)
            self.load(transformed_path)
            self.set_last_update_date()
        except Exception as e:
            logger.error(f'Error running pipeline for table {self.table_name}')
            raise e