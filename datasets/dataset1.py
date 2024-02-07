import re, os
import hashlib
import functools
import numpy as np
import pandas as pd
from tqdm import tqdm
from ast import literal_eval

import warnings
warnings.filterwarnings("ignore")

def _normalize_whitespace(s):
    return re.sub(r'(\s)\1{1,}', r'\1', s).strip().replace(' ,', ',').replace(' ;', ';').replace(' :', ':') 
  
def _rem(x):
    return x.replace("\r", " ")\
                          .replace("\t", " ")\
                          .replace("\n", " ")\
                          .replace("\\", "/")\
                          .replace("«", " ")\
                          .replace("»", " ")\
                          .replace("<", " ")\
                          .replace(">", " ")\
                          .replace("#", " ")\
                          .replace("№", " ")\
                          .replace(" -", "-")\
                          .replace("- ", "-")\
                          .replace(' "', '"')\
                          .replace('" ', '"')\
                          .replace("”", " ")\
                          .replace(',', ', ')\
                          .replace(';', '; ')\
                          .replace(':', ': ')

def _eval(x):
    if pd.notna(x):
      x = _rem(str(x))                        
      x = _normalize_whitespace(x)#.lower()
      x = literal_eval(x)
    return x

def _deduplicate(x):
    if isinstance(x, list):
      l = len(x)
      if l == 0:
        return np.nan
      if isinstance(x[0], dict):
        d = pd.DataFrame(x).drop_duplicates()
        dup = ~d.applymap(lambda x: ''.join([i for i in str(x).lower() if i.isalnum()])).duplicated()
        if len(d) > dup.sum():
          d = d[dup]        
        return d.to_dict('records')
      x = [i for i in x if pd.notna(i)]
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
  
def _check(x):
    x = dict(x)
    x.pop('id_candidate', None)
    x.pop('graduate_year', None)
    x.pop('date_from', None)
    x.pop('date_to', None)
    return bool(sum([len(str(i)) for i in x.values() if pd.notna(i)]) > 3)

@functools.cache
def _hash(x):
    return hashlib.shake_128(str(x).encode()).hexdigest(4)
  
@functools.cache
def _clean(x):
    return _normalize_whitespace(_rem(str(x)).replace("'", " ").replace('"', ' ')) if pd.notna(x) else x

def process_chunk(chunk):
    filtered = []
    # rehash
    chunk['id_candidate'] = chunk['id_candidate'].parallel_apply(_hash)
    # попытка удалить явные дубликаты словарей внутри CV и пустые списки   
    chunk['addedu'] = chunk['addedu'].parallel_apply(_deduplicate) 
    chunk['edu'] = chunk['edu'].parallel_apply(_deduplicate) 
    chunk['workexp'] = chunk['workexp'].parallel_apply(_deduplicate)
    chunk['position_name'] = chunk['position_name'].parallel_apply(_clean)
    
    for idx, subset in chunk.groupby(['id_candidate']):
              if len(subset) > 1: # если есть несколько CV, то собираем в одну запись всю информацию
                  subset = subset.to_dict('records') #sort_values(by='date_modify_inner_info').
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
              # фильтруем только кандидатов, указавших информацию
              if isinstance(item[0]['edu'], list) or isinstance(item[0]['addedu'], list) or isinstance(item[0]['workexp'], list):
                  del item[0]['id_cv'] 
                  # объединяем edu и addedu
                  if isinstance(item[0]['edu'], list) and isinstance(item[0]['addedu'], list):
                    item[0]['edu'] = item[0]['edu'] + item[0]['addedu']
                  elif not isinstance(item[0]['edu'], list) and isinstance(item[0]['addedu'], list):
                    item[0]['edu'] = item[0]['addedu'].copy()
                  del item[0]['addedu']
                  # оставляем наиболее позднюю дату изменения CV и собираем в строку другие атрибуты
                  if isinstance(item[0]['date_modify_inner_info'], list):
                      item[0]['date_modify_inner_info'] = _max(item[0]['date_modify_inner_info'])
                  if isinstance(item[0]['position_name'], list):
                      item[0]['position_name'] = ', '.join(item[0]['position_name'])
                  if isinstance(item[0]['salary'], list):
                      item[0]['salary'] = _max(item[0]['salary'])
                  if isinstance(item[0]['region_code'], list):
                      item[0]['region_code'] = ', '.join([str(int(i)) for i in item[0]['region_code']])
                  if isinstance(item[0]['gender'], list):
                      item[0]['gender'] = _max(item[0]['gender'])
                  if isinstance(item[0]['birthday'], list):
                      item[0]['birthday'] = _max(item[0]['birthday'])
                  filtered += item
    return filtered

if __name__ == '__main__':
    print('MAIN PROCESS')
                   
    base_dir = './'
    chunksize = 1000
    dataset_filename = 'dataset1.csv'
    os.makedirs(base_dir, exist_ok=True)
    from pandarallel import pandarallel
    pandarallel.initialize(progress_bar=False)
    
    edu_cols = ['id_candidate', 'legal_name', 'graduate_year', 'faculty', 'qualification', 'speciality', 'course_name', 'description']
    workexp_cols = ['id_candidate', 'company_name', 'date_from', 'date_to', 'job_title']
    clean_filename = os.path.join(base_dir, f"{dataset_filename}.clean.csv")
    cand_filename = os.path.join(base_dir, f"{dataset_filename}.cand.clean.csv")
    edu_filename = os.path.join(base_dir, f"{dataset_filename}.edu.clean.csv")
    workexp_filename = os.path.join(base_dir, f"{dataset_filename}.workexp.clean.csv")
    
    total_size = 0
    with pd.read_csv(os.path.join(base_dir, dataset_filename), 
                     chunksize=chunksize, sep='|', 
                     parse_dates=False, dtype={'salary': 'Int64',
                                                                            'gender': 'Int64',
                                                                            'birthday': 'Int64',
                                                                            'region_code': 'Int64',
                                                                            }, converters={'edu': _eval,
                                                                                        'addedu': _eval,
                                                                                        'workexp': _eval,
                                                                            }) as reader:
            for chunk in tqdm(reader):
                chunk = pd.DataFrame(process_chunk(chunk))
                
                if len(chunk):
                  chunk.to_csv(clean_filename,
                              header=(total_size==0), mode='a', sep='|', index=False)
                  
                  chunk.drop(['edu', 'workexp'], axis=1, errors='ignore').drop_duplicates().to_csv(cand_filename,
                              header=(total_size==0), mode='a', sep='|', index=False)
                  
                  for idx, items in chunk.iterrows():
                    if 'edu' in items and isinstance(items['edu'], (list, dict)):
                      edu = pd.DataFrame(items['edu'], dtype=str)
                      edu['id_candidate'] = items['id_candidate']
                      for c in edu_cols:
                        if c not in edu:
                          edu[c] = np.nan
                        else:
                          if 'date' not in c and 'id' not in c:
                            edu[c] = edu[c].apply(lambda x: _normalize_whitespace(str(x).replace("'", " ")) if pd.notna(x) else x)
                      edu = edu[edu_cols]
                      edu = edu[edu.parallel_apply(_check, axis=1)]
                      edu.drop_duplicates(inplace=True)
                      edu.to_csv(edu_filename,
                              header=(not os.path.exists(edu_filename)), mode='a', sep='|', index=False)
                      del edu
                      
                    if 'workexp' in items and isinstance(items['workexp'], (list, dict)):
                      workexp = pd.DataFrame(items['workexp'], dtype=str)
                      workexp['id_candidate'] = items['id_candidate']
                      for c in workexp_cols:
                        if c not in workexp:
                          workexp[c] = np.nan
                        else:
                          if 'date' not in c and 'id' not in c:
                            workexp[c] = workexp[c].apply(lambda x: _normalize_whitespace(str(x).replace("'", " ")) if pd.notna(x) else x)
                      workexp = workexp[workexp_cols]
                      workexp = workexp[workexp.parallel_apply(_check, axis=1)]
                      workexp.drop_duplicates(inplace=True)
                      workexp.to_csv(workexp_filename,
                              header=(not os.path.exists(workexp_filename)), mode='a', sep='|', index=False)
                      del workexp
                  
                  total_size += len(chunk)
                  del chunk
    print(f"total size: {total_size}")
    # total size: 6221448
