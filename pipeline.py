'''
Источник данных резюме:
creator "Портал Работа в России"
version "http://opendata.gosmonitor.ru/standard/3.0"
title "Резюме из ЕЦП «Работа в России»"
description "Аналитические данные по гражданам на портале «Работа в России». 
             В наборе представлены аналитические сведения по размещенным резюме на портале «Работа в России». 
             Данные обновляются ежедневно. Резюме формируются из личных кабинетов соискателей на портале. 
             Обращаем внимание, что поля с контактными данными соискателей, 
             а так же другая персональная информация в наборе не заполняются."
data link "https://opendata.trudvsem.ru/7710538364-cv/data-20231230T043004-structure-20161130T143000.xml"
structure "https://opendata.trudvsem.ru/7710538364-cv/structure-20231230T043004.xsd"
attributes "https://opendata.trudvsem.ru/7710538364-cv/attributes-20231230T043004.json"

Каталог с данными: "https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/"
Основные ссылки:
https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-cv/
https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-vacancy/
https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-professions/
https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-industries/
https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-regions/

'''

import os
from tqdm import tqdm
import pandas as pd
import sys

sys.path.append('./src')
from src.adding_tables_psycopg import AddingDataPsycopg
from src.renewal import Renewal
from src.utils import retreive_filelist

monthly=True # Обрабатывать только архивы на последний день месяца
remove_gz=False # Удалять ли gz архив после обработки, по умолчанию сохраняется
filelist_name = './filelist.csv' # Файл фиксирующий список ссылок и обработанных архивов (см. атрибут added)
base_url = 'https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/'
tables = {'curricula_vitae': '7710538364-cv/',
          'workexp': '7710538364-cv/',
          'edu': '7710538364-cv/',
          'addedu': '7710538364-cv/',
          'vacancies': '7710538364-vacancy/',
          'professions': '7710538364-professions/',
          'invitations': '7710538364-invitation/',
          'responses': '7710538364-response/',
          'industries': '7710538364-industries/',
          'regions':'7710538364-regions/', 
          }
    
filelist = None
if not os.path.exists(filelist_name):
    filelist = retreive_filelist(base_url, list(dict.fromkeys(tables.values())), monthly=monthly)
    filelist.sort_values(by='month' if monthly else 'date', ascending=False, inplace=True)
    filelist.to_csv(filelist_name, index=False)
else:    
    filelist = pd.read_csv(filelist_name) # список ссылок полученный с сайта
print(f"file list of {len(filelist)} files")

print(f"open database and create tables...")
db = AddingDataPsycopg()
for table in tables.keys():
  db.create_table(str(table), 'project_trudvsem') # 'project_trudvsem' - название схемы в БД, необходимо создать заранее
db.conn.close()
del db

print(f"starting process...")
for idx, link in tqdm(filelist.iterrows(), total=len(filelist)):
  if link['added']:
      continue
     
  if idx == filelist.index[0]: # выполняется только для наиболее позднего архива в списке (первая строка)
      for table in ['professions', 'industries', 'regions']:
        if pd.notna(link[table]):
          print(f'\n{table}')
          r = Renewal(table, base_url + tables[table] + link[table])
          try:
            r.download()
            r.extract()
            r.parse_update()
          except Exception as e:
            print(f'{table} error: {e}')
          r.delete(remove_gz=remove_gz)
          del r
        else:
          print(f'no link for {table}')

  for table in ['curricula_vitae', 'responses']: # 'invitations', 'vacancies'
      if pd.notna(link[table]):
          print(f'\n{table}')
          r = Renewal(table, base_url + tables[table] + link[table])
          try:
            r.download()
            r.extract()
            r.parse_update() # default_tables=['curricula_vitae', 'edu', 'addedu', 'workexp']
          except Exception as e:
            print(f'{table} error: {e}')
          r.delete(remove_gz=remove_gz)
          del r
      else:
          print(f'no link for {table}')  
  
  filelist.loc[idx, 'added'] = True
  filelist.to_csv(filelist_name, index=False)    
