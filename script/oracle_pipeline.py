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
from dotenv import load_dotenv


#dotenv
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


class OraclePipeline:
    def __init__(self, table_name = 'ARTLIF', exclude_columns = [], loaded_path = ''):
        try:
            logger = logging.getLogger(__name__ + '.__init__')
            logger.info(f'Initializing OraclePipeline')


            self.table_name = table_name
            #for since splitting load function
            self.loaded_path = loaded_path
            self.exclude_columns = exclude_columns 
            self.run_date = ''


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
            logger = logging.getLogger(__name__ + "." + self.table_name + '.get_last_update_date')
            cursor = self.mssql_conn.cursor()


            #get last update date from table LastUpdate
            logger.info(f'Getting last update date from table LastUpdate for table {self.table_name}')
            statement = f"SELECT Date FROM LastUpdate WHERE TableName = '{self.table_name}'"
            logger.debug(f'Statement: {statement}')

            cursor.execute(statement)
            #check if result is empty
            if cursor.rowcount == 0:
                self.set_last_update_date(new = 1)
                last_update_date = '2000-01-01'
            else:
                last_update_date = cursor.fetchone()[0]
            return last_update_date
        except Exception as e:
            logger.error(f'Error getting last update date from table LastUpdate: {e}')
            raise e



    #function that checks last update on on LastUpdate Table
    def set_last_update_date(self, new = 0):
        try:
            logger = logging.getLogger(__name__ + "." + self.table_name + '.set_last_update_date')
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
            logger = logging.getLogger(__name__ + "." + self.table_name + '.extract')
            cursor = self.oracle_conn.cursor()

            #get last update date from table LastUpdate
            entry_date = self.get_last_update_date()



            #get data from oracle db 
            
            logger.info(f'Getting data from oracle db where U_DATE >= {entry_date}')
            statement = f'SELECT * FROM DSPJTENERGY.{self.table_name} WHERE U_DATE >= TO_DATE(\'{entry_date}\', \'YYYY-MM-DD\')'
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
            logger.error(f'Error extracting from oracle db for table {self.table_name}: {e}')
            raise e





    #function that transforms the data
    def transform(self, raw_path):
        try:
            logger = logging.getLogger(__name__ + "." + self.table_name + '.transform')
            #if excluded columns is empty, run this code block
            transformed_path = f'data/transformed/{self.table_name}'
            create_directory(transformed_path)
            transformed_file_path = f'{transformed_path}/{self.table_name}_{self.run_date}_transformed.csv'
            if not self.exclude_columns: 
                #just move the raw to trasnformed folder to save space
                shutil.move(raw_path, transformed_file_path)
                return transformed_file_path
            else:
                #exclude columns from raw data
                logger.info(f'Excluding columns {self.exclude_columns} from raw data')
                #read csv to pandas dataframe
                df = pd.read_csv(raw_path, sep=',', encoding='utf-8')
                #drop excluded columns
                df.drop(self.exclude_columns, axis=1, inplace=True)
                #save to csv in transformed folder
                logger.info(f'Saving transformed data to csv in transformed folder: {transformed_file_path}')
                df.to_csv(transformed_file_path, index=False, encoding='utf-8')
                return transformed_file_path


        except Exception as e:
            logger.error(f'Error transforming data for table {self.table_name}: {e}')
            raise e
    
    #creates directories and reads csv to pandas dataframe
    def load(self, transformed_file_path):
        try:
            logger = logging.getLogger(__name__ + "." + self.table_name + '.setup_load')
            logger.info('Loading data to MSSQL DB')
            #create directory in loaded folder with table_name if it does not exist, 1 if created, 0 if already exists
            created = create_directory(self.loaded_path)


            #create table in mssql if it doesn't exist
            #val not being used 0 options: 0 = table already exists, 1 = altered, 2 = created
            val = self.create_table()

            #read csv to pandas dataframe
            data_df = pd.read_csv(transformed_file_path, sep=',', encoding='utf-8', engine='python')

            #load data to mssql table
            self.single_load(data_df)
        except Exception as e:
            logger.error(f'Error setting up load: {e}')
            raise e


    #bulk load function to use BULK INSERT in mssql WORK IN PROGRESS
    def bulk_load(self, data_df):
        try:
            logger = logging.getLogger(__name__ + "." + self.table_name + '.bulk_load')
            logger.info('Loading data to MSSQL DB')

            loaded_file_path = f'{self.loaded_path}/{self.table_name}_{self.run_date}_loaded.csv'
            mssql_cursor = self.mssql_conn.cursor()

            #get headers from csv
            headers = list(data_df.columns.values)

        except Exception as e:
            logger.error(f'Error loading data to MSSQL DB: {e}')
            raise e



    #function that loads the data to mssql
    def single_load(self, data_df):
        try:
            logger = logging.getLogger(__name__ + "." + self.table_name + '.single_load')
            logger.info('Loading data to MSSQL DB')
            
            loaded_file_path = f'{self.loaded_path}/{self.table_name}_{self.run_date}_loaded.csv'
            mssql_cursor = self.mssql_conn.cursor()
            #get headers from csv
            headers = list(data_df.columns.values)

            #write headers to csv in loaded folder, csv name with timestamp
            with open(loaded_file_path, 'w+', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
            
            #load data to mssql table
            for index, row in data_df.iterrows():

                #check if row already exists in table, POSNR is primary Key
                statement = f"SELECT * FROM {self.table_name} WHERE POSNR = '{row['POSNR']}'"
                logger.info(f'Checking if row with POSNR {row["POSNR"]} already exists..')
                logger.debug(f'Statement: {statement}')
                mssql_cursor.execute(statement)

                #if row exists, update row
                #if row exists, update row
                if mssql_cursor.fetchone() is not None:
                   
                    logger.info(f'Row with POSNR {row["POSNR"]} already exists, updating..')
                    


                    prepared_statement = f"UPDATE {self.table_name} SET "
                    for header in headers:
                        if pd.isna(row[header]):
                            prepared_statement += f"{header} = NULL, "
                        else:
                            prepared_statement += f"{header} = '{row[header]}', "

                    prepared_statement = prepared_statement[:-2]
                    prepared_statement += f" WHERE POSNR = '{row['POSNR']}'"
                    logger.debug(f'Prepared statement: {prepared_statement}')


                    mssql_cursor.execute(prepared_statement)
                    #commit changes to mssql db
                    self.mssql_conn.commit()

                    #save row to csv in loaded folder, csv name with timestamp
                    with open(loaded_file_path, 'a+', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(row)



                #if row does not exist, insert row
                else:
                    try:
                        #prepare values_string of values from row, if value is nan, replace with None
                        values_string = ''
                        for value in row:
                            if pd.isna(value):
                                values_string += 'NULL, '
                            else:
                                values_string += f"'{value}', "
                        values_string = values_string[:-2]

                        #prepare headers for sql statement
                        headers_string = ''
                        for header in headers:
                            headers_string += f'{header}, '
                        headers_string = headers_string[:-2]

                        #prepare values_string of values from row, if value is nan, replace with None
                        values_string = ''
                        for value in row:
                            if pd.isna(value):
                                values_string += 'NULL, '
                            else:
                                values_string += f"'{value}', "
                        values_string = values_string[:-2]

                        #prepare headers for sql statement
                        headers_string = ''
                        for header in headers:
                            headers_string += f'{header}, '
                        headers_string = headers_string[:-2]


                        #prepare statement
                        prepared_statement = f"INSERT INTO {self.table_name} ({headers_string}) VALUES ({values_string}) "
                        logger.info(f'Row with POSNR {row["POSNR"]} does not exist, inserting..')
                        logger.debug(f'Prepared statement: {prepared_statement}')


                        mssql_cursor.execute(prepared_statement)
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
            logger = logging.getLogger(__name__ + "." + self.table_name + '.create_table')
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
    def run_pipeline(self, table_name, exclude_columns = []):
        try:
            #setting table name
            self.table_name = table_name
            self.exclude_columns = exclude_columns
            self.run_date = datetime.datetime.now().strftime('%Y%d%m_%H%M%S')

            self.loaded_path = f'data/loaded/{self.table_name}'
            #run pipeline
            raw_path = self.extract()
            transformed_path = self.transform(raw_path)
            self.load(transformed_path)
            self.set_last_update_date()
        except Exception as e:
            logger.error(f'Error running pipeline for table {self.table_name}')
            raise e



    def run_load_again(self, table_name, old_transformed_file_path, old_loaded_file_path):
        try:
            self.table_name = table_name
            logger = logging.getLogger(__name__ + "." + self.table_name + '.run_load_again')
            logger.info(f'Running load again for table {self.table_name}')

           

            
            new_transformed_file_path = f'data/transformed/{self.table_name}_{self.run_date}_transformed_rerun.csv'
            #open old loaded file if exists
            if os.path.exists(old_loaded_file_path):


                #open old transformed file
                old_transformed_df = pd.read_csv(old_transformed_file_path, sep=',', encoding='utf-8', engine='python')
                #get POSNRs from old transformed file
                old_transformed_POSNRs = old_transformed_df['POSNR'].tolist()
                del old_transformed_df

                #open old loaded file
                old_loaded_df = pd.read_csv(old_loaded_file_path, sep=',', encoding='utf-8', engine='python')
                #get POSNRs from old loaded file
                old_loaded_POSNRs = old_loaded_df['POSNR'].tolist()
                del old_loaded_df
                


                #get POSNRs that are in old transformed file but not in old loaded file
                POSNRs_to_load = [posnr for posnr in old_transformed_POSNRs if posnr not in old_loaded_POSNRs]
                
                
                
                #get rows from old transformed file that have POSNRs that are not in old loaded file
                rows_to_load = old_transformed_df[old_transformed_df['POSNR'].isin(POSNRs_to_load)]

                logger.info(f'Saving leftover rows to csv in transformed folder: {new_transformed_file_path}')
                #save rows to csv in transformed_file, csv name with timestamp + transformed + rerun
                rows_to_load.to_csv(new_transformed_file_path, index=False, encoding='utf-8')
            else:
                logger.info(f'Old loaded file does not exist, copying old transformed file to new transformed file')
                #if old loaded file does not exist, just copy old transformed file to new transformed file
                shutil.copy(old_transformed_file_path, new_transformed_file_path)


            self.load(new_transformed_file_path)
            self.set_last_update_date()
        except Exception as e:
            logger.error(f'Error run load again for table {self.table_name}')
            raise e