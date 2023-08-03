from script.extract.extract import extract_dsp
from script.load.load import load
from script.transform.transform import transform_no_mod
from script.utils.get_set_last_update_date import get_last_update_date, set_last_update_date

def run():
    # Get last update date
    last_update_date = get_last_update_date('ARTDAT')
    raw_path = extract_dsp('DSPJTENERGY.ARTDAT', last_update_date)
    transformed_path = transform_no_mod(raw_path, 'DSPJTENERGY.ARTDAT_transformed')
    load(transformed_path, 'ARTDAT')
    set_last_update_date('ARTDAT')

