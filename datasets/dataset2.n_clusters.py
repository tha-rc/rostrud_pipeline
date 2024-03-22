import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from sklearn.cluster import KMeans
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
                   
    base_dir = './'
    dataset_filename = 'dataset2.csv'
    os.makedirs(base_dir, exist_ok=True)
    clean_filename_in = os.path.join(base_dir, f"{dataset_filename}.clean.clean.csv")
    
    
    data = pd.read_csv(clean_filename_in, sep='|', compression='zip', parse_dates=True)
    data['date_1'] = pd.to_numeric(pd.to_datetime(data.date_publish) - pd.to_datetime(data.date_creation))
    data['date_2'] = pd.to_numeric(pd.to_datetime(data.date_modify_inner_info) - pd.to_datetime(data.date_creation))
    data['age'] = 2023 - data.birthyear
    data = data[['age', 'gender', 'experience', 'busy_type', 'date_1', 'date_2', #'region_code', 
       'education_type', 'salary', 'responses', 'len_add_certificates_modified', 'len_skills',
       'len_additional_skills', 'len_other_info_modified', 'is_generated', 'cv_count']]
    
    print('n =', len(data))
    
    n = 10
    sil = []
    sca = StandardScaler()
    imp = SimpleImputer(missing_values=np.nan, strategy='mean')
    imp.fit(data)
    x_ = imp.transform(data)
    sca.fit(x_)
    x_ = sca.transform(x_)
    for s in tqdm([10000, 100000, 1000000]):
        for i in tqdm(range(2, n+1), leave=False):
            kmeans = KMeans(n_clusters=i, random_state=13, n_init='auto')
            sil.append((i, s, silhouette_score(x_, kmeans.fit_predict(x_), sample_size=s, random_state=13)))
    pd.DataFrame(sil)\
        .rename(columns={0: 'n_clusters', 1: 'sample_size', 2: 'silhouette_score'})\
            .to_csv(os.path.join(base_dir, 'dataset2.n_clusters.csv'), index=False)
