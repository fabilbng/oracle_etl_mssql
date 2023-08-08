from script.extract import extract
from script.load import load_to_mssql_variable_columns
from script.transform import transform_zbtab2
from script.utils.get_set_last_update_date import get_last_update_date, set_last_update_date


def run():
    table = 'ZBTAB2'
    last_update_date = get_last_update_date(table)
    raw_path = extract(f'DSPJTENERGY.{table}', last_update_date)
    transformed_path = transform_zbtab2(raw_path, f'DSPJTENERGY.{table}_transformed')
    load_to_mssql_variable_columns(transformed_path, table)
    set_last_update_date(table)

