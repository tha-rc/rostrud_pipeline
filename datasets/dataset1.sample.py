import os
import json
import bisect
import numpy as np
import pandas as pd
from tqdm import tqdm
from sklearn.model_selection import train_test_split

import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
                  
    base_dir = './'
    chunksize = 100000
    dataset_filename = 'dataset1.csv'
    os.makedirs(base_dir, exist_ok=True)

    clean_filename = os.path.join(base_dir, f"{dataset_filename}.clean.csv")
    index_file = os.path.join(base_dir, f"{dataset_filename}.index.csv")
    
    cand_filename = os.path.join(base_dir, f"{dataset_filename}.cand.clean.csv")
    edu_filename = os.path.join(base_dir, f"{dataset_filename}.edu.clean.csv")
    workexp_filename = os.path.join(base_dir, f"{dataset_filename}.workexp.clean.csv")
    
    total_size = 0
    if not os.path.exists(index_file):
        statistics = {}
        with pd.read_csv(clean_filename, 
                        chunksize=chunksize, sep='|', 
                        parse_dates=False, dtype=str) as reader:
                for chunk in tqdm(reader):
                    for idx, items in chunk.iterrows():
                        i = 0
                        if 'edu' in items and isinstance(items['edu'], (list, dict, str)):
                            i += len(str(items['edu']))
                        if 'workexp' in items and isinstance(items['workexp'], (list, dict, str)):
                            i += len(str(items['workexp']))
                        statistics[items['id_candidate']] = i
                    total_size += len(chunk)
                    del chunk
                    #break
        print(f"total size: {total_size}")
        statistics = pd.DataFrame().from_dict(statistics, orient='index').reset_index().rename(columns={0: 'value'})
        _, bins = np.histogram(statistics['value'], bins=500)
        statistics['bin'] = statistics['value'].apply(lambda x: bisect.bisect(bins, x) - 1)
        
        filter = statistics['bin'].value_counts()
        filter = set(filter[filter > 3].index)

        statistics = statistics[statistics['bin'].apply(lambda x: x in filter)]
        _, sample = train_test_split(statistics, test_size=min(len(filter), 200), random_state=13, stratify=statistics['bin'])
        sample = sample['index'].to_list()
        with open(index_file, 'w') as f:
            json.dump(sample, f)
        del statistics
        del filter
        del bins
    else:
        with open(index_file, 'r') as f:
            sample = json.load(f)
    sample = set(sample)
    print(f"sample size: {len(sample)}")

    cand = pd.read_csv(cand_filename, sep='|', parse_dates=False, dtype=str)
    cand[cand.id_candidate.apply(lambda x: x in sample)].to_csv(f"{cand_filename}.sample.csv",
                                                                    sep='|', index=False)
    del cand
    
    edu = pd.read_csv(edu_filename, sep='|', parse_dates=False, dtype=str)
    edu[edu.id_candidate.apply(lambda x: x in sample)].to_csv(f"{edu_filename}.sample.csv",
                                                                    sep='|', index=False)
    del edu
    
    workexp = pd.read_csv(workexp_filename, sep='|', parse_dates=False, dtype=str)
    workexp[workexp.id_candidate.apply(lambda x: x in sample)].to_csv(f"{workexp_filename}.sample.csv",
                                                                    sep='|', index=False)
    del workexp