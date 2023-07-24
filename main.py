from script.extract import extract
from script.utils.loggingSetup import setup_logger
from script.transform import transform_no_mod
from script.load import load_to_mssql_variable_columns

def main():
    setup_logger()
    raw_path = extract('DSPJTENERGY.ARTDAT')
    transformed_path = transform_no_mod(raw_path, 'DSPJTENERGY.ARTDAT_transformed')

    print(transformed_path)
    load_to_mssql_variable_columns(transformed_path, 'ARTDAT')

main()