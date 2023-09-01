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



class OraclePipeline:
    def __init__(self):
        try:
            logger = logging.getLogger(__name__ + '.__init__')
            logger.info(f'Initializing OraclePipeline')
            logger.debug('Connecting to oracle database')
            self.oracle_conn = oracledb.connect(user=oracle_un, password=oracle_pw, dsn=oracle_db)


            logger.debug('Connecting to mssql database')
            connect_string_2  = f'DRIVER=SQL Server;SERVER={mssql_dbs};DATABASE={mssql_db};UID={mssql_un};PWD={mssql_pw}'
            connect_string_1 = f'DRIVER=SQL Server;SERVER={mssql_dbs};DATABASE={mssql_db};trusted=1'
            if env == 'dev':
                self.mssql_conn = pyodbc.connect(connect_string_1)
            elif env == 'prod':
                self.mssql_conn = pyodbc.connect(connect_string_2)
            else:
                raise ValueError('Wrong value given for env (either dev or prod)')


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
                logger.debug(f'Inserting new row in table LastUpdate for table {self.table_name}')
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
            statement = f'SELECT * FROM DSPJTENERGY.{self.table_name} WHERE U_DATE >= TO_DATE(\'{entry_date}\', \'YYYY-MM-DD\')'
            logger.debug(f'Getting data from oracle db where U_DATE >= {entry_date}')
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


            
            
            
            #raw path file: table name + timestamp + .csv
            raw_file_path = f'{self.raw_path}/{self.table_name}_{self.run_date}.csv'

            
            #save data to csv in raw folder
            logger.debug(f'Saving data to csv in raw folder: {raw_file_path}')
            with open(raw_file_path, 'w+', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                #create array of header names
                headers = []
                for column in data_columns:
                    headers.append(column[0])
                # Write the header
                writer.writerow(headers)
                # Write the rows
                writer.writerows(data)


           
            table_info_file_path = f'{self.table_info_path}/{self.table_name}_oracle_table_info.csv'


            #save table info to csv in raw folder
            logger.debug(f'Saving table info to csv in table info folder: {table_info_file_path}')
            with open(table_info_file_path, 'w+', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                #create array of header names

                
                headers = []
                for column in table_info_columns:
                    headers.append(column[0])
                # Write the header
                writer.writerow(headers)
                # Write the rows
                writer.writerows(table_info)


            return raw_file_path
        except Exception as e:
            logger.error(f'Error extracting from oracle db for table {self.table_name}: {e}')
            raise e





    #function that transforms the data
    def transform(self, raw_path):
        try:
            logger = logging.getLogger(__name__ + "." + self.table_name + '.transform')
            logger.info('Transforming data')
            #if excluded columns is empty, run this code block
            transformed_file_path = f'{self.transformed_path}/{self.table_name}_{self.run_date}_transformed.csv'
            if not self.exclude_columns: 
                #move raw file to transformed folder
                logger.debug(f'Moving raw file to transformed folder: {transformed_file_path}')
                shutil.move(raw_path, transformed_file_path)

                return transformed_file_path
            else:
                #exclude columns from raw data
                logger.debug(f'Excluding columns {self.exclude_columns} from raw data')
                #read csv to pandas dataframe
                df = pd.read_csv(raw_path, sep=',', encoding='utf-8')
                #drop excluded columns
                df.drop(self.exclude_columns, axis=1, inplace=True)
                #save to csv in transformed folder
                logger.debug(f'Saving transformed data to csv in transformed folder: {transformed_file_path}')
                df.to_csv(transformed_file_path, index=False)
                return transformed_file_path


        except Exception as e:
            logger.error(f'Error transforming data for table {self.table_name}: {e}')
            raise e
    
    #creates directories and reads csv to pandas dataframe
    def load(self, transformed_file_path):
        try:
            logger = logging.getLogger(__name__ + "." + self.table_name + '.setup_load')
            logger.info('Loading data to MSSQL DB')
            


            #create table in mssql if it doesn't exist
            #val not being used 0 options: 0 = table already exists, 1 = altered, 2 = created
            val = self.create_table()


            #read csv to pandas dataframe
            data_df = pd.read_csv(transformed_file_path, sep=',', encoding='utf-8', engine='python')


            #open loaded file path and updated file path and insert header
            logger.debug("Creating loaded and updated csv files with headers")
            loaded_file_path = f'{self.loaded_path}/{self.table_name}_{self.run_date}_loaded.csv'
            updated_file_path = f'{self.loaded_path}/{self.table_name}_{self.run_date}_updated.csv'
            with open(loaded_file_path, 'w+', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(data_df.columns.values)
            with open(updated_file_path, 'w+', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(data_df.columns.values)


            #TESTING BULK LOAD
            self.bulk_load(data_df)

        except Exception as e:
            logger.error(f'Error setting up load: {e}')
            raise e




    #bulk load function to use BULK INSERT in mssql WORK IN PROGRESS
    def bulk_load(self, data_df):
        try:
            logger = logging.getLogger(__name__ + "." + self.table_name + '.bulk_load')

            if data_df.empty:
                logger.info('No new data since last run...')
                return
            
            logger.info('Loading data to MSSQL DB')
            

            loaded_file_path = f'{self.loaded_path}/{self.table_name}_{self.run_date}_loaded.csv'
            mssql_cursor = self.mssql_conn.cursor()

            #get headers from csv
            headers = list(data_df.columns.values)
            
            #rowcount dataframe
            rowcount = data_df.shape[0]
            rows_per_chunk = 100

            #divide dataframe into 20 row chunks
            #get number of chunks
            chunks = rowcount // rows_per_chunk

            #get remainder
            remainder = rowcount % rows_per_chunk

            #if remainder is not 0, add 1 to chunks
            if remainder != 0:
                chunks += 1
            
            #get chunks of dataframe
            data_df_chunks = np.array_split(data_df, chunks)

            #load data to mssql table
            for data_df_chunk in data_df_chunks:
                try:
                    #get POSNR from dataframe
                    POSNR = data_df_chunk['POSNR']

                    #check database for POSNRs
                    statement = f"SELECT POSNR FROM {self.table_name} WHERE POSNR IN ({','.join(POSNR.astype(str))})"
                    logger.debug(f'Checking if rows with POSNRs already exists..')
                    logger.debug(f'Statement: {statement}')
                    mssql_cursor.execute(statement)
                    #get POSNRs that already exist in database
                    POSNRs_in_db = mssql_cursor.fetchall()
                    #turn into int list
                    POSNRs_in_db = [int(i[0]) for i in POSNRs_in_db]
                    
                   
                    
                    data_df_in_db = data_df_chunk[data_df_chunk['POSNR'].isin(POSNRs_in_db)]
                    data_df_not_in_db = data_df_chunk[~data_df_chunk['POSNR'].isin(POSNRs_in_db)]
                   
                    
                    #Prepare Sql
                    #prepare headers for sql statement
                    headers_string = ''
                    for header in headers:
                        headers_string += f'{header}, '
                    headers_string = headers_string[:-2]


                    #HANDLING ROWS THAT DONT EXIST IN DB
                    #check if data_df_not_in_db is defined
                    if data_df_not_in_db.empty:
                        logger.debug(f'All rows with POSNRs {POSNRs_in_db} already exist..')

                    elif data_df_not_in_db.shape[0] >= 1:
                        #prepare values_string of values from row, if value is nan, replace with NULL
                        values_string = ''
                        for index, row in data_df_not_in_db.iterrows():
                            for value in row:
                                if pd.isna(value):
                                    values_string += 'NULL, '
                                else:
                                    values_string += f"'{value}', "
                            values_string = values_string[:-2]
                            values_string += '), ('
                        values_string = values_string[:-4]

                        #prepare statement
                        rowcount = rowcount - data_df_not_in_db.shape[0]
                        logger.info(f'Inserting {data_df_not_in_db.shape[0]} rows that do not exist in DB: {rowcount} rows left')
                        prepared_statement = f"INSERT INTO {self.table_name} ({headers_string}) VALUES ({values_string}) "
                        logger.debug(f'Prepared statement: {prepared_statement}')
                        #execute prepared statement
                        mssql_cursor.execute(prepared_statement)
                        self.mssql_conn.commit()
                        #save data_df_not_in_db to csv in loaded folder without header, csv name with timestamp
                        with open(loaded_file_path, 'a+', newline='', encoding='utf-8') as f:
                            writer = csv.writer(f)
                            writer.writerows(data_df_not_in_db.values)



                    #HANDLING ROWS THAT EXIST IN DB
                    #check if data_df_in_db is not empty
                    if data_df_in_db.empty:
                        logger.debug(f'No rows with POSNRs {POSNRs_in_db} already exist..')

                    elif data_df_in_db.shape[0] >= 1:
                        #update rows
                        for index, row in data_df_in_db.iterrows():
                            self.update_row(row, headers)
            
                except Exception as e:
                    logger.error(f'Error loading chunk to MSSQL DB, single loading...')
                    self.single_load(data_df_chunk)

    
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
                logger.debug(f'Checking if row with POSNR {row["POSNR"]} already exists..')
                logger.debug(f'Statement: {statement}')
                mssql_cursor.execute(statement)

                #if row exists, update row
                #if row exists, update row
                if mssql_cursor.fetchone() is not None:
                    logger.info(f'Row with POSNR {row["POSNR"]} already exists, updating..')
                    self.update_row(row, headers)


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
                        
                        failed_loaded_file_path = f'{self.failed_loaded_path}/{self.table_name}_{self.run_date}_failed_inserts.csv'

                        
                        #save failed row to csv in failed_inserts folder
                        with open(failed_loaded_file_path, 'a+', newline='', encoding='utf-8') as f:
                            writer = csv.writer(f)
                            writer.writerow(row)


            logger.info('Data successfully loaded to MSSQL DB')
        except Exception as e:
            logger.error(f'Error loading data to MSSQL DB: {e}')
            raise e
    

    #function that takes a single  dataframe row and updates it in mssql
    def update_row(self, row, headers):
        try:
            logger = logging.getLogger(__name__ + "." + self.table_name + '.update_row')
            mssql_cursor = self.mssql_conn.cursor()
            prepared_statement = f"UPDATE {self.table_name} SET "
            for header in headers:
                if pd.isna(row[header]):
                    prepared_statement += f"{header} = NULL, "
                else:
                    prepared_statement += f"{header} = '{row[header]}', "

            prepared_statement = prepared_statement[:-2]
            prepared_statement += f" WHERE POSNR = '{row['POSNR']}'"
            logger.info(f'Row with POSNR {row["POSNR"]} already exists, updating..')
            logger.debug(f'Prepared statement: {prepared_statement}')
            mssql_cursor.execute(prepared_statement)
            self.mssql_conn.commit()

            #save row to csv in updated folder, csv name with timestamp
            updated_path = f'data/loaded/{self.table_name}'
            updated_file_path = f'{updated_path}/{self.table_name}_{self.run_date}_updated.csv'
            created = create_directory(updated_path)
            #save failed row to csv in failed_inserts folder
            with open(updated_file_path, 'a+', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(row)
        
        except Exception as e:
            logger.error(f'Error updating data to MSSQL DB: {e}')
            failed_update_path = f'data/loaded/{self.table_name}/failed_inserts'
            failed_update_file_path = f'{failed_update_path}/{self.table_name}_{self.run_date}_failed_update.csv'
            created = create_directory(failed_update_path)
            #save failed row to csv in failed_inserts folder
            with open(failed_update_file_path, 'a+', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(row)
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
            logger.debug(f'Checking if table {self.table_name} exists in mssql')
            statement = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{self.table_name}'"
            logger.debug(f'Statement: {statement}')
            mssql_cursor = self.mssql_conn.cursor()
            mssql_cursor.execute(statement)
            table_exists = mssql_cursor.fetchone()



            #if table exists, check if table structure is the same
            if table_exists:
                logger.debug(f'Table {self.table_name} already exists in mssql, checking if strcuture is the same')
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
                    logger.debug(f'Table {self.table_name} structure is the same, no need to alter table')
                    return 0 #return 0 if table structure is the same
                else:
                    logger.debug(f'Table {self.table_name} structure is different, altering table')
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
                    logger.debug(f'Statement: {alter_table_statement}')
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
        logger = logging.getLogger(__name__ + "." + table_name + '.run_pipeline')
        try: 
            logger.debug(f'Running pipeline for table {table_name}')

            #setting table name
            self.table_name = table_name
            self.exclude_columns = exclude_columns

            #creating neccessary folders
            self.raw_path = create_directory(f'data/raw/{self.table_name}')
            self.transformed_path = create_directory(f'data/transformed/{self.table_name}')
            self.loaded_path =  create_directory(f'data/loaded/{self.table_name}')
            self.table_info_path = create_directory(f'data/table_info/{self.table_name}')
            self.failed_loaded_path = create_directory(f'data/loaded/{self.table_name}/failed_inserts')

            #run pipeline
            self.run_date = datetime.datetime.now().strftime('%Y%d%m_%H%M%S')

            raw_file_path = self.extract()

            transformed_file_path = self.transform(raw_file_path)
            
            self.load(transformed_file_path)
            self.set_last_update_date()
            logger.debug(f'Pipeline for table {self.table_name} successfully ran')
        except Exception as e:
            logger.error(f'Error running pipeline for table {self.table_name}')
            raise e
