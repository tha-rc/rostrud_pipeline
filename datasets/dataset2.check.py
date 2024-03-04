import os
#import hashlib
#import functools
#import numpy as np
import pandas as pd
#from tqdm import tqdm
#from ast import literal_eval

import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
                   
    base_dir = './'
    dataset_filename = 'dataset2.csv'
    os.makedirs(base_dir, exist_ok=True)

    clean_filename_in = os.path.join(base_dir, f"{dataset_filename}.clean.csv")
    clean_filename_out = os.path.join(base_dir, f"{dataset_filename}.clean.clean.csv")

    data = pd.read_csv(os.path.join(base_dir, clean_filename_in), sep='|', parse_dates=False, dtype=str)
    print(f'size of {clean_filename_in}: {len(data)}')
    dup_idx = data.id_candidate.duplicated(keep=False)
    dup = data[dup_idx].copy()
    data = data[~dup_idx]
    print(f'duplicated {clean_filename_in}: {len(dup)}')
    dup.sort_values(by=['birthday', 'date_modify_inner_info'], ascending=True, inplace=True)
    dup = dup[~dup.id_candidate.duplicated()]
    data = pd.concat([data, dup], ignore_index=True).rename(columns={'birthday' : 'birthyear'})
    data.to_csv(os.path.join(base_dir, clean_filename_out), sep='|', index=False)
    print(f'size of {clean_filename_out}: {len(data)}')
    del dup
    del data
    # 7262277