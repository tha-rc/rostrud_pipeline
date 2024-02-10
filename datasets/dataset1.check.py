import re, os
import hashlib
import functools
import numpy as np
import pandas as pd
from tqdm import tqdm
from ast import literal_eval

import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
                   
    base_dir = './'
    dataset_filename = 'dataset1.csv'
    os.makedirs(base_dir, exist_ok=True)

    cand_filename_in = os.path.join(base_dir, f"{dataset_filename}.cand.clean.csv")
    cand_filename_out = os.path.join(base_dir, f"{dataset_filename}.cand.csv")   
    edu_filename_in = os.path.join(base_dir, f"{dataset_filename}.edu.clean.csv")
    workexp_filename_in = os.path.join(base_dir, f"{dataset_filename}.workexp.clean.csv")
    edu_filename_out = os.path.join(base_dir, f"{dataset_filename}.edu.csv")
    workexp_filename_out = os.path.join(base_dir, f"{dataset_filename}.workexp.csv")  


    data = pd.read_csv(os.path.join(base_dir, cand_filename_in), sep='|', parse_dates=False, dtype=str)
    print(f'size of {cand_filename_in}: {len(data)}')
    dup_idx = data.id_candidate.duplicated(keep=False)
    dup = data[dup_idx].copy()
    data = data[~dup_idx]
    print(f'duplicated {cand_filename_in}: {len(dup)}')
    dup.sort_values(by='date_modify_inner_info', ascending=True, inplace=True)
    dup = dup[dup.id_candidate.duplicated()]
    data = pd.concat([data, dup], ignore_index=True)
    data.to_csv(os.path.join(base_dir, cand_filename_out), sep='|', index=False)
    print(f'size of {cand_filename_out}: {len(data)}')
    del dup
    del data
    # 6221439

    data = pd.read_csv(os.path.join(base_dir, edu_filename_in), sep='|', parse_dates=False, dtype=str)
    print(f'size of {edu_filename_in}: {len(data)}')
    dup_idx = data.duplicated(keep=False)
    print(f'duplicated {edu_filename_in}: {dup_idx.sum()}')
    data.drop_duplicates(inplace=True)
    print(f'size of {edu_filename_out}: {len(data)}')
    data.to_csv(os.path.join(base_dir, edu_filename_out), sep='|', index=False)
    del data
    # 9124525

    data = pd.read_csv(os.path.join(base_dir, workexp_filename_in), sep='|', parse_dates=False, dtype=str)
    print(f'size of {workexp_filename_in}: {len(data)}')
    dup_idx = data.duplicated(keep=False)
    print(f'duplicated {workexp_filename_in}: {dup_idx.sum()}')
    data.drop_duplicates(inplace=True)
    print(f'size of {workexp_filename_out}: {len(data)}')
    data.to_csv(os.path.join(base_dir, workexp_filename_out), sep='|', index=False)
    del data
    # 10254450
