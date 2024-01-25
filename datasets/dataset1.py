import re, os
import numpy as np
import pandas as pd
from tqdm import tqdm
from ast import literal_eval

import warnings
warnings.filterwarnings("ignore")

def _normalize_whitespace(s):
    return re.sub(r'(\s)\1{1,}', r'\1', s)

def _eval(x):
    if pd.notna(x):
      x = str(x).replace("\r", " ")\
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
                          .replace('" ', '"')                          
      x = _normalize_whitespace(x).strip()#.lower()
      x = literal_eval(x)
    return x

def _deduplicate(x):
    if isinstance(x, list):
      l = len(x)
      if l == 0:
        return np.nan
      if isinstance(x[0], dict):
        d = pd.DataFrame(x).drop_duplicates()
        d_ = ~d.applymap(str.lower).duplicated()
        if len(d) > d_.sum():
          d = d[d_]        
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

def process_chunk(chunk):
    filtered = []
    # попытка удалить явные дубликаты словарей внутри CV и пустые списки   
    chunk['addedu'] = chunk['addedu'].parallel_apply(_deduplicate) 
    chunk['edu'] = chunk['edu'].parallel_apply(_deduplicate) 
    chunk['workexp'] = chunk['workexp'].parallel_apply(_deduplicate)
    chunk['position_name'] = chunk['position_name'].str.lower()
    
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
                    item[0]['edu'] = item[0]['addedu']
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
    chunksize = 100000
    dataset_filename = 'dataset1.csv'
    os.makedirs(base_dir, exist_ok=True)
    from pandarallel import pandarallel
    pandarallel.initialize(progress_bar=False)
    
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
                chunk.to_csv(os.path.join(base_dir, f"{dataset_filename}.clean.csv"),
                            header=(total_size==0), mode='a', sep='|', index=False)
                total_size += len(chunk)
                del chunk
    print(f"total size: {total_size}")
    # total size: 6221448
