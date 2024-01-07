import gzip, os
#import glob
import shutil
import pickle
import requests
from os import listdir
import pandas as pd
from parsing_xmls import *


# Получаем дату из имени файла
def get_date(url):
    return url.split('data-')[-1].split('T')[0] #use regexp to get the date of the file

# def get_date_year_ago(date_ymd):
#     year_ago = int(date_ymd.rsplit('-')[0]) - 1
#     return str(year_ago) + date_ymd[4:]

# Разархивируем файл в определённый путь
def unzip_cv(path):
    with gzip.open(path, 'rb') as f_in:
        with open(path[:-3], 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

# Выбрать парсер по переданному имени обновляемой таблицы
def get_parser(table_name):#, path=None):
    if table_name == 'curricula_vitae':
        return ParseCvs()
    if table_name == 'invitations':
        return ParseInvitations()
    if table_name == 'vacancies':
        return ParseVacancies()
    if table_name == 'responses':
        return ParseResponses()
    if table_name == 'organizations':
        return ParseOrganizations()
    if table_name == 'regions':
        return ParseRegions()
    if table_name == 'industries':
        return ParseIndustries()
    if table_name == 'professions':
        return ParseProfessions()
    if table_name == 'stat_companies':
        return ParseStatCompany()
    if table_name == 'stat_citizens':
        return ParseStatCitizens()
    return None
    
def save_pickle(df, file_name):
    with open(file_name, 'wb') as f:
        pickle.dump(df, f) 
        
def load_pickle(file_name):
    with open(file_name, 'rb') as f:
        return pickle.load(f)
    
# Создать датафрейм из csv с типом данных 'object'
def read_csv_object(filepath):
    return pd.read_csv(filepath, dtype='object')

# Создать датафрейм из коллекции .csv файлов, тип данных 'object', сортировка по алфавиту, обнуляя индекс
def make_df(dirpath):
    filepaths = [dirpath + "/" + f for f in listdir(dirpath) if f.endswith('.csv')]
    return pd.concat(map(read_csv_object, filepaths), sort=True).reset_index(drop=True)

# Добавить кавычки при создании строки из списка
def to_str_wquotes(lst):
    return ', '.join(f"'{w}'" for w in lst)

def get_csv_files(dirpath):
    #file_list = glob.glob(dirpath + "/*.csv")
    filepaths = [dirpath + "/" + f for f in listdir(dirpath) if f.endswith('.csv')]
    if len(filepaths) and os.path.exists(filepaths[0]):
        return filepaths
    return []
    
def rem_csv_files(filepaths):
    #file_list = glob.glob(dirpath + "/*.csv")
    #filepaths = [dirpath + "/" + f for f in listdir(dirpath) if f.endswith('.csv')]
    if len(filepaths) and os.path.exists(filepaths[0]):
        for f in filepaths:
            os.remove(f)
            
def get_pickle_files(dirpath):
    filepaths = [dirpath + "/" + f for f in listdir(dirpath) if f.endswith('.pickle')]
    if len(filepaths) and os.path.exists(filepaths[0]):
        return filepaths
    return []
    
def rem_pickle_files(filepaths):
    if len(filepaths) and os.path.exists(filepaths[0]):
        for f in filepaths:
            os.remove(f)

def retreive_filelist(base_url, file_url, monthly=True, start_from=2019):
    # print(file_url)
    # last file in each month
    def _getfileslist(url):
      r = requests.get(url)
      return ['data' + i.split('">data')[0] for i in r.text.split('<a href="data')[1:]]
    
    s = pd.DataFrame(_getfileslist(base_url + file_url[0])).rename(columns={0: 'cv'})
    s = s[s['cv'].apply(lambda x: pd.notna(x) and '.gz' in x)]
    s = s[s['cv'].apply(lambda x: int(x.split('data-')[1][:4]) >= start_from)]

    if monthly:
      s['month'] = s['cv'].apply(lambda x: x.split('data-')[1][:6])
      s = s.groupby('month').agg('last').reset_index()
      for l in file_url[1:]:
          c = l.split('-')[1]
          c = c[:len(c)-1]
          v = pd.DataFrame(pd.Series(_getfileslist(base_url + l)).rename(c))
          v = v[v[c].apply(lambda x: pd.notna(x) and '.gz' in x)]
          v = v[v[c].apply(lambda x: int(x.split('data-')[1][:4]) >= start_from)]
          v['month'] = v[c].apply(lambda x: x.split('data-')[1][:6])
          v = v.groupby('month').agg('last').reset_index()
          s = s.set_index('month').join(v.set_index('month')).reset_index()
    else:
    # all files
      s['date'] = s['cv'].apply(lambda x: x.split('data-')[1][:8])
      s = s.groupby('date').agg('last').reset_index()
      for l in file_url[1:]:
          c = l.split('-')[1]
          c = c[:len(c)-1]
          v = pd.DataFrame(pd.Series(_getfileslist(base_url + l)).rename(c))
          v = v[v[c].apply(lambda x: pd.notna(x) and '.gz' in x)]
          v = v[v[c].apply(lambda x: int(x.split('data-')[1][:4]) >= start_from)]
          v['date'] = v[c].apply(lambda x: x.split('data-')[1][:8])
          v = v.groupby('date').agg('last').reset_index()
          s = s.set_index('date').join(v.set_index('date')).reset_index()
    #s.to_csv('filtered_all.csv', index=False)
    s['added'] = False
    return s.rename(columns={'cv' : 'curricula_vitae',
                             'vacancy' : 'vacancies',
                             'invitation' : 'invitations',
                             'response' : 'responses'})