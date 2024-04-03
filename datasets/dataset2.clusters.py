import os
import random
import pandas as pd
import numpy as np
from kPOD import k_pod

import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
                   
    base_dir = './'
    dataset_filename = 'dataset2.csv'
    os.makedirs(base_dir, exist_ok=True)
    clean_filename_in = os.path.join(base_dir, f"{dataset_filename}.clean.clean.csv")
    clean_filename_out = os.path.join(base_dir, f"{dataset_filename}.cluster.csv")
    
    
    data = pd.read_csv(clean_filename_in, sep='|', parse_dates=True)
    data['date_1'] = pd.to_numeric(pd.to_datetime(data.date_publish) - pd.to_datetime(data.date_creation))
    data['date_2'] = pd.to_numeric(pd.to_datetime(data.date_modify_inner_info) - pd.to_datetime(data.date_creation))
    #data['age'] = 2023 - data.birthyear
    x = data[['date_1', 'date_2', 
       #'education_type', 'salary', 'region_code', 'age', 'gender', 'experience', 'busy_type', 
       'responses', 'len_add_certificates_modified', 'len_skills',
       'len_additional_skills', 'len_other_info_modified', 'is_generated', 'cv_count']]
    
    print('n =', len(x))
    
    random.seed(13)
    np.random.seed(13)
    results = k_pod(x, 2) # https://pypi.org/project/kPOD/
    results = pd.Series(results[0])
    data = pd.concat([data, results.rename('cluster')], axis=1)[['id_candidate', 'region_code', 'birthyear', 'gender', 'experience',
       'education_type', 'busy_type', 'salary', 'responses', 'date_creation', 'date_publish', 'date_modify_inner_info', 'is_generated', 'cluster']]
    data.astype({'salary': 'Int64', 'responses': 'Int64',
                                                'gender': 'Int64', 'experience': 'Int64',
                                                'birthyear': 'Int64', 'education_type': 'Int64',
                                                'busy_type': 'Int64', 'is_generated': 'Int64',
                                                'cluster': 'Int64',
                                                }).to_csv(clean_filename_out, index=False, sep='|')
