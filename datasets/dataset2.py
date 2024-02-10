import os
import hashlib
import functools
import numpy as np
import pandas as pd
from tqdm import tqdm

import warnings
warnings.filterwarnings("ignore")

def _deduplicate(x):
    if isinstance(x, list):
      l = len(x)
      if l == 0:
        return np.nan
      if isinstance(x[0], dict):
        return pd.DataFrame(x).drop_duplicates().to_dict('records')
      x = list(dict.fromkeys(x))
      if len(x) == 1:
        return x[0]
    return x

def _max(x):
    if isinstance(x, list):
      x = [i for i in x if pd.notna(i)]
      if len(x):
        x = max(x)
      else:
        x = np.nan
    return x 
  
@functools.cache
def _hash(x):
    return hashlib.shake_128(str(x).encode()).hexdigest(10)

def process_chunk(chunk):
    filtered = []
    # rehash
    # chunk['id_candidate'] = chunk['id_candidate'].parallel_apply(_hash)
    
    for idx, subset in chunk.groupby(['id_candidate']):
            
            if len(subset) > 1: # если есть несколько CV, то собираем в одну запись всю информацию
                  subset = subset.to_dict('records')
                  item = {k : [] for k in subset[0].keys()}
                  for i in subset:
                    for k in item.keys():
                      if isinstance(i[k], list):
                        item[k] += i[k]
                      elif pd.notna(i[k]):
                        item[k] += [i[k]]
                  # очередня попытка удалить явные дубликаты словарей, появившиеся при объединении CV
                  item = [{k : _deduplicate(v) for k, v in item.items()}] 
            else:
                  item = subset.to_dict('records') # здесь только одно CV

            if isinstance(item[0]['id_cv'], list):
                item[0]['cv_count'] = len(item[0]['id_cv'])
            else:
                item[0]['cv_count'] = 1
            del item[0]['id_cv']
            # оставляем наиболее позднюю дату изменения CV и собираем в строку другие атрибуты
            if isinstance(item[0]['date_modify_inner_info'], list):
                      item[0]['date_modify_inner_info'] = _max(item[0]['date_modify_inner_info'])
            if isinstance(item[0]['date_publish'], list):
                      item[0]['date_publish'] = _max(item[0]['date_publish'])
            if isinstance(item[0]['date_creation'], list):
                      item[0]['date_creation'] = _max(item[0]['date_creation'])
            if isinstance(item[0]['date_last_updated'], list):
                      item[0]['date_last_updated'] = _max(item[0]['date_last_updated'])
            if isinstance(item[0]['salary'], list):
                      item[0]['salary'] = _max(item[0]['salary'])
            if isinstance(item[0]['experience'], list):
                      item[0]['experience'] = _max(item[0]['experience'])
            if isinstance(item[0]['is_generated'], list):
                      item[0]['is_generated'] = _max(item[0]['is_generated'])
            if isinstance(item[0]['education_type'], list):
                      item[0]['education_type'] = _max(item[0]['education_type'])
            if isinstance(item[0]['busy_type'], list):
                      item[0]['busy_type'] = _max(item[0]['busy_type'])
            if isinstance(item[0]['responses'], list):
                      item[0]['responses'] = _max(item[0]['responses'])
            if isinstance(item[0]['gender'], list):
                      item[0]['gender'] = _max(item[0]['gender'])
            if isinstance(item[0]['birthday'], list):
                      item[0]['birthday'] = _max(item[0]['birthday'])        
            if isinstance(item[0]['region_code'], list):
                     item[0]['region_code'] = ', '.join([str(int(i)) for i in item[0]['region_code']])                     
            if isinstance(item[0]['len_add_certificates_modified'], list):
                      item[0]['len_add_certificates_modified'] = _max(item[0]['len_add_certificates_modified'])
            if isinstance(item[0]['len_skills'], list):
                      item[0]['len_skills'] = _max(item[0]['len_skills'])
            if isinstance(item[0]['len_additional_skills'], list):
                      item[0]['len_additional_skills'] = _max(item[0]['len_additional_skills'])
            if isinstance(item[0]['len_other_info_modified'], list):
                      item[0]['len_other_info_modified'] = _max(item[0]['len_other_info_modified'])
            filtered += item
    return filtered

if __name__ == '__main__':
    print('MAIN PROCESS')
                   
    base_dir = './'
    chunksize = 200000
    dataset_filename = 'dataset2.csv'
    os.makedirs(base_dir, exist_ok=True)

    from pandarallel import pandarallel
    pandarallel.initialize(progress_bar=False)

    allowed_cols = ['id_candidate', 'birthday', 'gender', 'experience', 
                    'busy_type', 'education_type', 'region_code', 'salary',
                    'date_creation', 'date_publish', 'date_modify_inner_info', 'date_last_updated',
                    'responses', 'len_add_certificates_modified', 'len_skills', 'len_additional_skills',
                    'len_other_info_modified', 'is_generated', 'cv_count']
    
    total_size = 0
    with pd.read_csv(os.path.join(base_dir, dataset_filename), 
                     chunksize=chunksize, sep='|', 
                     parse_dates=False, dtype={'salary': 'Int64', 'responses': 'Int64',
                                                'gender': 'Int64', 'experience': 'Int64',
                                                'birthday': 'Int64', 'education_type': 'Int64',
                                                'busy_type': 'Int64', 'is_generated': 'Int64',
                                                'len_add_certificates_modified': 'Int64',
                                                'len_skills': 'Int64', 'len_additional_skills': 'Int64',
                                                'len_other_info_modified': 'Int64',
                                                'region_code': 'Int64'}) as reader:
            for chunk in tqdm(reader):
                chunk = pd.DataFrame(process_chunk(chunk))
                if len(chunk) and 'id_candidate' in chunk:
                  chunk[allowed_cols].drop_duplicates().astype({'salary': 'Int64', 'responses': 'Int64',
                                                'gender': 'Int64', 'experience': 'Int64',
                                                'birthday': 'Int64', 'education_type': 'Int64',
                                                'busy_type': 'Int64', 'is_generated': 'Int64',
                                                'len_add_certificates_modified': 'Int64',
                                                'len_skills': 'Int64', 'len_additional_skills': 'Int64',
                                                'len_other_info_modified': 'Int64', 'cv_count': 'Int64',
                                                #'region_code': str,
                                                }).to_csv(os.path.join(base_dir, f"{dataset_filename}.clean.csv"),
                            header=(total_size==0), mode='a', sep='|', index=False)
                total_size += len(chunk)
                del chunk
    print(f"total size: {total_size}")
    # 24761724
    # 22582133
    # 23290531
    # 23290984