from script.extract import extract
from script.load import load_to_mssql_variable_columns
from script.transform import transform_zbtab2


def run():
    raw_path = extract('DSPJTENERGY.ZBTAB2')
    transformed_path = transform_zbtab2(raw_path, 'DSPJTENERGY.ZBTAB2_transformed')
    load_to_mssql_variable_columns(transformed_path, 'ZBTAB2')


