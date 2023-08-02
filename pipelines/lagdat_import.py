from script.extract import extract
from script.load import load_to_mssql_variable_columns
from script.transform import transform_no_mod


def run():
    raw_path = extract('DSPJTENERGY.LAGDAT')
    transformed_path = transform_no_mod(raw_path, 'DSPJTENERGY.LAGDAT_transformed')
    load_to_mssql_variable_columns(transformed_path, 'LAGDAT')

