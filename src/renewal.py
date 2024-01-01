import wget
import os
import sys
#sys.path.append('./process')
import shutil
import pandas as pd
import hashlib
import pickle
import parsing_xmls
from conf import Config
from utils import get_date, unzip_cv, get_parser, make_df, to_str_wquotes, get_csv_files, rem_csv_files, read_csv_object, get_pickle_files, rem_pickle_files
from adding_tables_psycopg import AddingDataPsycopg
from process import process
from geti import hashes, update_hashes
from adding_tables_psycopg_rostrud import *

def save_pickle(df, file_name):
    with open(file_name, 'wb') as f:
        pickle.dump(df, f) 
        
def load_pickle(file_name):
    with open(file_name, 'rb') as f:
        return pickle.load(f)

# в url можно передавать только ссылки с датой (пример: 
# https://opendata.trudvsem.ru/oda2Hialoephidohyie1oR6chaem1oN0quiephooleiWei1aiD/7710538364-cv/data-20220204T044647-structure-20161130T143000.xml.gz)
class Renewal:
    """ Функции класса последовательно скачивают xml файл с ftp, парсят нужные данные, 
    обновляют БД, удаляют временные файлы и папки в бакете"""
    def __init__(self, table_name, url):
        self.name = table_name
        self.pathfrom = url
        # в случаях передачи в класс ссылки без даты (напр. regions.xml) в self.date попадает вся ссылка
        # здесь дата в формате 20180101 (имеет смысл исправить!)
        raw_date = get_date(self.pathfrom)
        self.date = raw_date[:4] + '-' + raw_date[4:6] + '-' + raw_date[6:]
        self.workdir = Config(os.path.join('./src/', 'all_tables_names.yml')).get_config('working_directory')
        
        self.datadir = os.path.join(self.workdir, table_name)
        if not os.path.isdir(self.datadir):
            os.makedirs(self.datadir, exist_ok=True)
        self.pathxml = os.path.join(self.datadir, self.date + self.name + '.xml')
    
    def _wget(self, url, out, retry=5):
        for i in range(1, retry+1):
            try:
                wget.download(url, out)
                return
            except Exception as e:
                print(f'wget error: {e}, retry: {i}')   
                
    def _check_cols(self, table_name):
        #fill undefined cols
        self.col_names = Config(os.path.join('./src/', 'all_tables_names.yml')).get_config('create_table')[table_name]
        self.col_types = [i.split()[1].lower() for i in self.col_names.split(',')] 
        self.col_names = [i.split()[0] for i in self.col_names.split(',')] 
                   
        for i, c in enumerate(self.col_names):
            if c not in self.df.columns:
                if 'int' in self.col_types[i]:
                    self.df[c] = 0
                elif 'bool' in self.col_types[i]:
                    self.df[c] = False
                else:
                    self.df[c] = None
            '''
            else:
                if 'int' in self.col_types[i]:
                    self.df[c] = self.df[c].astype(int)
                elif 'bool' in self.col_types[i]:
                    self.df[c] = self.df[c].astype(bool)
                else:
                    pass  
            '''     
    
    def _write_to_bd(self, table_name):
        #открывается соединение с БД
        add_data = AddingDataPsycopg()
        # записываем в БД
        add_data.write_to_sql(self.df[self.col_names], table_name, 'project_trudvsem')
        #print(f"{table}({csv_file}) добавлено {self.df.shape[0]} строк")
        add_data.conn.close()  
    
    def download(self):
        print('downloading xml/gz files...', self.date)
        if self.pathfrom.rsplit('.', maxsplit=1)[-1] == 'gz':
            pathgz = self.pathxml + '.gz'
            if not os.path.exists(pathgz):
                #wget.download(self.pathfrom, pathgz)
                self._wget(self.pathfrom, pathgz)
                #unzip_cv(pathgz)
        if self.pathfrom.rsplit('.', maxsplit=1)[-1] == 'xml':
            if not os.path.exists(self.pathxml):
                #wget.download(self.pathfrom, self.pathxml)
                self._wget(self.pathfrom, self.pathxml)
        print('xml/gz files are downloaded:', self.date)
        
    def extract(self):    
        print('extracting gz files...', self.date)            
        if not os.path.exists(self.pathxml):
            pathgz = self.pathxml + '.gz'
            unzip_cv(pathgz)
        print('xml/gz files are extracted:', self.date)
            
    def parse_update(self, default_tables=['curricula_vitae', 'workexp', 'edu', 'addedu']):
        print(self.date)
        '''
        to_delete = ['stat_citizens', 'industries', 'professions', 'regions', 'stat_companies']
        if self.name in to_delete:
            add_data = AddingDataPsycopg()
            add_data.delete_table(self.name, 'project_trudvsem')
            add_data.create_table(self.name, 'project_trudvsem')
            add_data.conn.close()
            parser = get_parser(self.name)
            parser.to_csvs()
            self.df = make_df(self.datadir)
            self.df = process(self.name, self.df)
            
            #fill undefined cols
            self.col_names = Config(os.path.join('.', 'rostrud_ml/utils/all_tables_names.yml')).get_config('create_table')[self.name] ##
            self.col_names = [i.split()[0] for i in self.col_names.split(',')]            
            for c in self.col_names:
                if c not in self.df.columns:
                    self.df[c] = None
            
            #открывается соединение с БД
            add_data = AddingDataPsycopg()
            add_data.write_to_sql(self.df[self.col_names], self.name, 'project_trudvsem')
            print(f"{self.name}({csv_file}) добавлено {self.df.shape[0]} строк")
            add_data.conn.close()
        '''
        if self.name == 'curricula_vitae':   
        #elif self.name == 'curricula_vitae':
            parser = get_parser(self.name, self.pathxml)
            for table in default_tables: #
                #self.df = 
                parser.to_csvs(table)
                #csv_files = get_csv_files(self.datadir)
                csv_files = get_pickle_files(self.datadir)
                for csv_file in csv_files:
                    try:
                        #self.df = read_csv_object(csv_file)
                        self.df = load_pickle(csv_file)
                        #self._check_cols(table)
                        self.df = process(table, self.df)
                        print(f"{table}({csv_file}) is processed")
                        
                        #fill undefined cols
                        self._check_cols(table)
                        #self.col_names = Config(os.path.join('.', 'rostrud_ml/utils/all_tables_names.yml')).get_config('create_table')[table] ##
                        #self.col_names = [i.split()[0] for i in self.col_names.split(',')]            
                        #for c in self.col_names:
                        #    if c not in self.df.columns:
                        #        self.df[c] = None                        
                        '''
                        #открывается соединение с БД
                        add_data = AddingDataPsycopg()
                        # записываем в БД
                        add_data.write_to_sql(self.df[self.col_names], table, 'project_trudvsem')
                        print(f"{table}({csv_file}) добавлено {self.df.shape[0]} строк")
                        add_data.conn.close()
                        '''
                        self._write_to_bd(table)
                        print(f"{table}({csv_file}) добавлено {self.df.shape[0]} строк")
                        
                    #except Exception as e:
                    #    print(f'{table}({csv_file}) error: {e}')
                        
                    finally:
                        pass
                #rem_csv_files(csv_files)
                rem_pickle_files(csv_files)
                print("temporary files deleted")  
            '''            
        elif self.name == 'vacancies':
            parser = get_parser(self.name)
            parser.to_csvs(self.name)
            print(self.date)
            '''  
            '''
            #открывается соединение с БД
            add_data = AddingDataPsycopg()
            old_hash_set = add_data.get_table_as_df('md5_hash, inactive', 'vacancies', 'project_trudvsem')
            old_hash_list = old_hash_set['md5_hash'].tolist()
            self.df = make_df(self.datadir)
            variables_list = Config(os.path.join('.', 'rostrud_ml/utils/all_tables_names.yml')).get_config('md5_hash')
            variables = variables_list['vacancies']
            self.df['md5_hash'] = self.df[variables].apply(lambda row: hashlib.md5(','.join(row.values.astype(str)).encode()).hexdigest(), axis=1)
            #при необходимости можно использовать версионирование
            #new_hash_list = self.df['md5_hash'].tolist()
            self.df = self.df.loc[~self.df.md5_hash.isin(old_hash_list), :]
            #active_hash_list = old_hash_set['md5_hash'][old_hash_set['inactive'] == 0].tolist()
            #inactive_hash_list = old_hash_set['md5_hash'][old_hash_set['inactive'] == 1].tolist()
            #inactive = set(active_hash_list).difference(set(new_hash_list))
            #same = set(inactive_hash_list).intersection(set(new_hash_list))
            #print('Проставить флаг неактивности: ', len(inactive))
            #print('Проверить стали ли неактивные активными: ', len(same))
            self.df = process('vacancies', self.df)
            add_data.write_to_sql(self.df, self.name, 'project_trudvsem')
            print(self.name + ": добавлено: ", self.df.shape[0], ' строк')
            # обновим флаги inactive
            #if len(inactive) > 0:
            #    update_inactivation_new('project_trudvsem', 'vacancies', self.date, to_str_wquotes(inactive))
            #if len(same) > 0: 
            #    fix_error_inactivation_new('project_trudvsem', 'vacancies', to_str_wquotes(same))
            add_data.conn.close()
            '''
            '''
            #csv_files = get_csv_files(self.datadir)
            csv_files = get_pickle_files(self.datadir)
            for csv_file in csv_files:
                    try:
                        #self.df = read_csv_object(csv_file)
                        self.df = load_pickle(csv_file)
                        self.df = process(self.name, self.df)
                        #print(self.name, ": обработано")
                        print(f"{self.name}({csv_file}) is processed")
                        
                        #fill undefined cols
                        self.col_names = Config(os.path.join('.', 'rostrud_ml/utils/all_tables_names.yml')).get_config('create_table')[self.name] ##
                        self.col_names = [i.split()[0] for i in self.col_names.split(',')]            
                        for c in self.col_names:
                            if c not in self.df.columns:
                                self.df[c] = None
                        
                        #открывается соединение с БД
                        add_data = AddingDataPsycopg()
                        # записываем в БД
                        add_data.write_to_sql(self.df[self.col_names], self.name, 'project_trudvsem')
                        #print(self.name + ": добавлено: ", self.df.shape[0], ' строк')
                        print(f"{self.name}({csv_file}) добавлено {self.df.shape[0]} строк")
                        add_data.conn.close()
                    except Exception as e:
                        print(f'{self.name}({csv_file}) error: {e}')                    
            #rem_csv_files(csv_files)
            rem_pickle_files(csv_files)
            print("temporary files deleted")            
            ''' 
        else:
            parser = get_parser(self.name)
            parser.to_csvs()
            #self.df = make_df(self.datadir)
            #self.df = process(self.name, self.df)
            ##открывается соединение с БД
            #add_data = AddingDataPsycopg()
            #add_data.write_to_sql(self.df, self.name, 'project_trudvsem')
            #print(self.name + ": добавлено: ", self.df.shape[0], ' строк')
            #add_data.conn.close()
            
            #csv_files = get_csv_files(self.datadir)
            csv_files = get_pickle_files(self.datadir)
            for csv_file in csv_files:
                    try:
                        #self.df = read_csv_object(csv_file)
                        self.df = load_pickle(csv_file)
                        #self._check_cols(self.name)
                        self.df = process(self.name, self.df)
                        #print(self.name, ": обработано")
                        print(f"{self.name}({csv_file}) is processed")
                        
                        #fill undefined cols
                        self._check_cols(self.name)
                        #self.col_names = Config(os.path.join('.', 'rostrud_ml/utils/all_tables_names.yml')).get_config('create_table')[self.name] ##
                        #self.col_names = [i.split()[0] for i in self.col_names.split(',')]            
                        #for c in self.col_names:
                        #    if c not in self.df.columns:
                        #        self.df[c] = None   
                        '''
                        #открывается соединение с БД
                        add_data = AddingDataPsycopg()
                        # записываем в БД
                        add_data.write_to_sql(self.df[self.col_names], self.name, 'project_trudvsem')
                        #print(self.name + ": добавлено: ", self.df.shape[0], ' строк')
                        print(f"{self.name}({csv_file}) добавлено {self.df.shape[0]} строк")
                        add_data.conn.close()
                        '''
                        #print(self.df.accommodation_capability.value_counts())
                        
                        self._write_to_bd(self.name)
                        print(f"{self.name}({csv_file}) добавлено {self.df.shape[0]} строк")
                        
                    #finally:
                    #    pass
                    except Exception as e:
                        print(f'{self.name}({csv_file}) error: {e}')                    
            #rem_csv_files(csv_files)
            rem_pickle_files(csv_files)
            print("temporary files deleted")
                       
    # удаление директории со всем содержимым       
    def delete(self, to_remove='files', remove_gz=True):
        if to_remove == 'files':
            pathgz = self.pathxml + '.gz'
            if os.path.exists(pathgz) and os.path.exists(self.pathxml):
                os.remove(self.pathxml)
            if os.path.exists(pathgz) and os.path.getsize(pathgz) > 0:
                if remove_gz:
                    os.remove(pathgz)
        
        else: 
            shutil.rmtree(self.datadir)
        print("xml/gz files deleted")
