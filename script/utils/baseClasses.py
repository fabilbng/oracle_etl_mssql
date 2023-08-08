import sys
sys.path.append('..')
import pyodbc
import oracledb
from script.utils.loggingSetup import log_error, log_info, log_warning




class MSSQLConnector:
    def __init__(self, mssql_dbs, mssql_db, mssql_un='', mssql_pw='', trusted=1):
        try:
            log_info('Connecting to MSSQL')
            if trusted == 1:
                conn_str = f'DRIVER={{SQL Server}};SERVER={mssql_dbs};DATABASE={mssql_db}'
            elif trusted == 0:
                conn_str = f'DRIVER={{SQL Server}};SERVER={mssql_dbs};DATABASE={mssql_db};UID={mssql_un};PWD={mssql_pw}'
            else:
                raise ValueError('Wrong value given for trusted (either 1 or 0)')
            
            self.mssql_conn = pyodbc.connect(conn_str)
            self.mssql_cursor = self.mssql_conn.cursor()
            log_info(f'Successfully connected to MSSQL database {mssql_db}')
        except Exception as e:
            log_error(f'Error initializing MSSQLConnector: {e}')
            raise e




class OracleDBConnector:
    def __init__(self, oracle_db, oracle_un, oracle_pw):
        try:
            log_info('Connecting to Oracle DB')
            self.oracle_conn = oracledb.connect(user=oracle_un, password=oracle_pw, dsn=oracle_db)
            self.oracle_cursor = self.oracle_conn.cursor()
            log_info(f'Successfully connected to Oracle DB {oracle_db}')
        except Exception as e:
            log_error(f'Error initializing OracleDBConnector: {e}')
            raise e