from script.extract.extract import extract_dsp
from script.load.load import load
from script.transform.transform import transform_zbtab2
from script.utils.get_set_last_update_date import get_last_update_date, set_last_update_date


def run():
    table = 'ZBTAB2'
    last_update_date = get_last_update_date(table)
    raw_path = extract_dsp(f'{table}', last_update_date)
    transformed_path = transform_zbtab2(raw_path, f'{table}_transformed')
    load(transformed_path, table)
    set_last_update_date(table)

