import os
#import hashlib
#import functools
import numpy as np
import pandas as pd
#from tqdm import tqdm
#from ast import literal_eval

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
    dup.sort_values(by=['birthday', 'date_modify_inner_info'], ascending=True, inplace=True)
    dup = dup[~dup.id_candidate.duplicated()]
    data = pd.concat([data, dup], ignore_index=True)
    data.to_csv(os.path.join(base_dir, cand_filename_out), sep='|', index=False)
    print(f'size of {cand_filename_out}: {len(data)}')
    cand_id = data[['id_candidate', 'birthday']]
    del dup
    del data
    # 6221439

    data = pd.read_csv(os.path.join(base_dir, edu_filename_in), sep='|', parse_dates=False, dtype=str)
    print(f'size of {edu_filename_in}: {len(data)}')
    dup_idx = data.duplicated(keep=False)
    print(f'duplicated {edu_filename_in}: {dup_idx.sum()}')
    data.drop_duplicates(inplace=True)
    print(f'size of {edu_filename_out}: {len(data)}')

    print(f"NA in graduate year: {pd.isna(data['graduate_year']).sum()}")
    data = data.set_index('id_candidate').join(cand_id.set_index('id_candidate'))
    data['graduate_year'] = data.apply(lambda x: x['graduate_year'] if pd.isna(x['birthday']) or (
                                                                            pd.notna(x['graduate_year']) and
                                                                            pd.notna(x['birthday']) and
                                                                            float(x['graduate_year']) > float(x['birthday']) + 10) else np.nan, axis=1)
    print(f"NA in graduate year: {pd.isna(data['graduate_year']).sum()}")

    data.reset_index().drop('birthday', axis=1).to_csv(os.path.join(base_dir, edu_filename_out), sep='|', index=False)
    del data
    del cand_id
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
