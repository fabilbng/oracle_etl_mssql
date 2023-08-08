from script.utils.loggingSetup import log_error, log_info, log_warning
import os
import time
import pandas as pd

#place where you would alter/transform the data, right now only saves the data to a new csv file in the trasnformed folder
#can implement new trasnformfunction if needed
def transform_no_mod(raw_path, transformed_path_name):
    #move from raw to transformed folder
    try:
        #save transformed path in variable (transformed_path_name + timestamp.csv)
        transformed_path = f'data/transformed/{transformed_path_name}'
        #saving file name in variable (transformed_path_name + timestamp.csv)
        file_name = f'{transformed_path_name}_{time.strftime("%Y%d%m_%H%M%S")}.csv'
        #saving full path in variable
        full_transformed_path = os.path.join(transformed_path, file_name)
        #checking if directory with transformed_path_name exists
        if not os.path.exists(transformed_path):
            os.makedirs(transformed_path)
    
       

        #move file from raw to transformed folder
        os.rename(raw_path, full_transformed_path)
        log_info('Data successfully moved from raw to transformed folder')
        return full_transformed_path
    except Exception as e:
        log_error(f'Error transforming data: {e}')
        raise e
    





def transform_zbtab2(raw_path, transformed_path_name):
    #open csv file in path with pandas and remove columns vartext1, vartext2
    #save new csv file in transformed folder
    try:
        #save transformed path in variable (transformed_path_name + timestamp.csv)
        transformed_path = f'data/transformed/{transformed_path_name}'
        #saving file name in variable (transformed_path_name + timestamp.csv)
        file_name = f'{transformed_path_name}_{time.strftime("%Y%d%m_%H%M%S")}.csv'
        #saving full path in variable
        full_transformed_path = os.path.join(transformed_path, file_name)

        #checking if directory with transformed_path_name exists
        if not os.path.exists(transformed_path):
            os.makedirs(transformed_path)
    
        #open csv file in path with pandas and remove columns vartext1, vartext2
        df = pd.read_csv(raw_path)
        df = df.drop(columns=['VARTEXT1', 'VARTEXT2', 'VARTEXT3'])
        df.to_csv(full_transformed_path, index=False)
        log_info('Data successfully transformed and saved to csv')
        return full_transformed_path
    except Exception as e: 
        log_error(f'Error transforming data: {e}')
        raise e