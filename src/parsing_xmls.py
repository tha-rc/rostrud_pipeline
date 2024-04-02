import os, sys
import pandas as pd
import pickle
import hashlib
from tqdm import tqdm
from lxml import etree
from adding_tables_psycopg import AddingDataPsycopg
from geti import hashes, create_md5
from conf import Config

def save_pickle(df, file_name):
    if len(df) > 0:
        with open(file_name, 'wb') as f:
            pickle.dump(df, f) 
        
def load_pickle(file_name):
    with open(file_name, 'rb') as f:
        return pickle.load(f)

class Parse:
    def __init__(self, table_name, chunk_size=250000):
        self.name = table_name
        self.workdir = Config(os.path.join('./src/', 'all_tables_names.yml')).get_config('working_directory')
        self.variables_list = Config(os.path.join('./src/', 'all_tables_names.yml')).get_config('md5_hash')   ##
        self.datadir = os.path.join(self.workdir, self.name)
        if not os.path.isdir(self.datadir):
            print('Отсутствует директория: ', self.datadir)
        self.filenamexml = [f for f in os.listdir(self.datadir) if f.endswith('.xml')][0]
        self.date = self.filenamexml[:10]
        self.pathxml = os.path.join(self.datadir, self.filenamexml)
        self.csv_size = chunk_size
        
# Парсинг файла с резюме    
class ParseCvs(Parse):
    def __init__(self):
        Parse.__init__(self, 'curricula_vitae')
        self.md5hash_variables = self.variables_list['curricula_vitae']   ##
        
    def to_csvs(self, table):
        if table == 'curricula_vitae':
# Резюме
            df = pd.DataFrame() #пустой датафрейм
            l = [] #пустой список
            i = 1
            # получаем список хешей всех ранее загруженных записей:
            #old_hash_set = set()#hashes(table)
            old_hash_set = hashes(table)
            # пустой список для новых хешей при необходимости версионирования
            #new_hash_list = []
        #recover позволяет обходить ParseError: not well-formed (invalid token)
            for event, elem in tqdm(etree.iterparse(self.pathxml, tag='cv', recover=True)):
                d = {} # Создаём словарь имя переменной: значение
                d['date_last_updated'] = self.date # Добавляем переменную с датой выгрузки файла
                d['id_cv'] = elem.attrib['about'].rsplit('#', maxsplit=1)[-1] # id находится в ссылке атрибута about
                for element in list(elem):  # Проходимся по каждому элементу в наборе
                    if element.tag == 'link':
                        d[element.attrib['resource'].rsplit('#', maxsplit=1)[0].\
                          rsplit('/', maxsplit=1)[-1].rsplit('.', maxsplit=1)[0]] = \
                        element.attrib['resource'].rsplit('#', maxsplit=1)[-1]
                    elif element.tag == 'profession':
                        d['profession_code'] = element.attrib['resource'].rsplit('#', maxsplit=1)[-1]
                    elif element.tag == 'region':
                        d['regions'] = element.attrib['resource'].rsplit('#', maxsplit=1)[-1]
                    elif element.tag == 'industry':
                        d['industries'] = element.attrib['resource'].rsplit('#', maxsplit=1)[-1]
                    elif element.tag == 'workExperienceList' or \
                    element.tag == 'educationList' or \
                    element.tag == 'additionalEducationList':
                        continue

                    elif len(list(element)) >= 1:  # Если длина значений этого элемента больше или равна 1, 
                                                    # то перед нами словарь
                        for sub_element in list(element):  # По которому тоже нужно пройтись
                            if len(list(sub_element)) >= 1:  
                                for sub_sub_element in list(sub_element): 
                                    d[element.tag + '_' + sub_element.tag + '_' + sub_sub_element.tag] = sub_sub_element.text

                            elif sub_element.text != None:
                                d[element.tag + '_' + sub_element.tag] = sub_element.text

                    elif element.text != None: # теги-родители без текста будут пропущены
                        d[element.tag] = element.text 

                    else:
                        continue

                elem.clear() # И очищаем память удаляя элемент

                # также надо удалить все ссылки (построенное в памяти дерево) перед этим элементом
                for ancestor in elem.xpath('ancestor-or-self::*'):
                    while ancestor.getprevious() is not None:
                        del ancestor.getparent()[0]
                        
                #добавим переменную с хеш-суммой строки         
                md5_hash = create_md5(d, added=self.md5hash_variables) #########################################################################3
                if md5_hash not in old_hash_set:
                    old_hash_set.add(md5_hash)
                    #при необходимости версионирования
                    #new_hash_list.append(md5_hash)
                    #добавим переменную с хеш-суммой строки 
                    d['md5_hash'] = md5_hash
                    l.append(d)  # Добавим этот словарик в список
                    #print(d)
                    #break
            #update_hashes(table, old_hash_set, new_hash_list) 

                    #print(pd.DataFrame(l).columns)
                    #print(pd.DataFrame(l)['stateRegionCode'])
                    #kk = pd.DataFrame(l)
                    #print(pd.to_numeric(kk.birthday, errors='coerce').fillna(1990))
                    #print(kk.birthday.astype(str).apply(lambda x: x[:4]).astype(int))
                    #print(kk)
                    #if 'birthday' in kk.columns:
                    #    print(kk['birthday'])
                    #print(kk['hardSkills'] + ', ' + kk['softSkills'])
                    
                    
                    if len(l) == self.csv_size: 
                        df = pd.DataFrame(l)
                        #print(df.columns)

                        #df.to_csv(self.datadir + f'/{table}{i}.csv', index=False)
                        save_pickle(df, self.datadir + f'/{table}{i}.pickle')
                        
                        i = i + 1
                        l = []
                        df = pd.DataFrame()
                        
            df = pd.DataFrame(l)
            #df.to_csv(self.datadir + f'/{table}.csv', index=False)
            
            save_pickle(df, self.datadir + f'/{table}.pickle')
            print(f'{table} is prepared')
            #return df

# Опыт работы, Образование, Дополнительное образование:
        else:
            #print(table)
            #sys.exit()
            tags_list = {'workexp': "workExperienceList",
                    'edu': "educationList", #education
                    'addedu': "additionalEducationList"} #additionalEducation
            #tags = {'workexp': ["workExperience"],
            #        'edu': ["educationType", "education"],
            #        'addedu': ["addEducation", "additionalEducation"],
            #}
            df = pd.DataFrame()
            l = []
            #j = 1
            i = 1
            # получаем список хешей всех ранее загруженных записей:
            old_hash_set = hashes(table)
            #old_hash_set = set()
            
            #from itertools import islice
            #with open(self.pathxml, encoding='utf-8') as input_file:
            #    head = list(islice(input_file, 10))
            #print(head)
            #sys.exit()
            # пустой список для новых хешей
            #new_hash_list = []
            for event, elem in tqdm(etree.iterparse(self.pathxml, tag='cv', recover=True)): #tags[table]
                # для каждой записи с нужным тегом создаётся хеш-сумма и сравнивается с имеющимися
                id_cv = elem.attrib['about'].rsplit('#', maxsplit=1)[-1] # id находится в ссылке атрибута about
                #print(id_cv)
                for element in list(elem):  # Проходимся по каждому элементу в наборе
                    if element.tag == tags_list[table]:
                        #print(tags_list[table], len(list(element)))
                        if len(list(element)) >= 1:
                            for sub_element in list(element):
                                #print(sub_element.tag)
                                md5_hash = hashlib.md5(etree.tostring(sub_element, encoding='UTF-8')).hexdigest()
                                if md5_hash not in old_hash_set:
                                    old_hash_set.add(md5_hash)
                                    d = {}
                                    d['md5_hash'] = md5_hash
                                    d['id_cv'] = id_cv
                                    d['date_last_updated'] = self.date
                                    for sub_sub_element in list(sub_element):
                                        d[sub_sub_element.tag] = sub_sub_element.text
                                    l.append(d)
                                    #print(d)
                                    #sys.exit()
                                    if len(l) == self.csv_size: 
                                        df = pd.DataFrame(l)
                                        #print(l)
                                        #break
                                        #df.to_csv(self.datadir + f'/{table}{i}.csv', index=False)
                                        save_pickle(df, self.datadir + f'/{table}{i}.pickle')
                                        i = i + 1
                                        l = []
                                        df = pd.DataFrame()
                
                elem.clear()
                for ancestor in elem.xpath('ancestor-or-self::*'):
                    while ancestor.getprevious() is not None:
                        del ancestor.getparent()[0]       
                                        

                    #for ancestor in elem.xpath('ancestor-or-self::*'):
                    #    while ancestor.getprevious() is not None:
                    #        del ancestor.getparent()[0]

                #for element in list(elem): 
                #    print(element.tag, element.text)
                #print(elem.iter)
            #sys.exit()
            '''
                md5_hash = hashlib.md5(etree.tostring(elem, encoding='UTF-8')).hexdigest()
                if md5_hash not in old_hash_set:
                    old_hash_set.add(md5_hash)
                    #new_hash_list.append(md5_hash)
                    d = {}
                    d['date_last_updated'] = self.date
                    for element in list(elem): 
                        d[element.tag] = element.text 
                    #добавим переменную с хеш-суммой строки 
                    d['md5_hash'] = md5_hash

                    elem.clear()

                    for ancestor in elem.xpath('ancestor-or-self::*'):
                        while ancestor.getprevious() is not None:
                            del ancestor.getparent()[0]

                    l.append(d)
                    #print(d)
                    #break
                    #j += 1
                    #if j >= 100:
                    #    break
                    
                    print(pd.DataFrame(l).columns)
                    break

                    if len(l) == self.csv_size: 
                        df = pd.DataFrame(l)
                        #print(l)
                        #break
                        #df.to_csv(self.datadir + f'/{table}{i}.csv', index=False)
                        save_pickle(df, self.datadir + f'/{table}{i}.pickle')
                        i = i + 1
                        l = []
                        df = pd.DataFrame()
            '''
                        
            df = pd.DataFrame(l)
            #df.to_csv(self.datadir + f'/{table}.csv', index=False)
            save_pickle(df, self.datadir + f'/{table}.pickle')
            #print(f'df {table} готов')
            print(f'{table} is prepared')

            #update_hashes(table, old_hash_set, new_hash_list)
            #df = pd.DataFrame(l)
            # передаём датафрейм в Renewal
            #return df

# Парсинг файла с вакансиями
class ParseVacancies(Parse):
    def __init__(self):
        Parse.__init__(self, 'vacancies')
        self.md5hash_variables = self.variables_list['vacancies'] ##
        
    def to_csvs(self):
        df = pd.DataFrame()
        i = 1
        l = []
        old_hash_set = hashes(self.name)
        
        for event, elem in tqdm(etree.iterparse(self.pathxml, tag='vacancy', recover=True)):
            
            d = {}
            d['date_last_updated'] = self.date

            for element in list(elem): # Проходимся по каждому элементу в наборе
                if element.tag == 'region':
                    d['region'] = element.attrib['resource'].rsplit('#', maxsplit=1)[-1]
                    continue
                elif element.tag == 'profession':
                    d['profession'] = element.attrib['resource'].rsplit('#', maxsplit=1)[-1]
                    continue
                elif element.tag == 'industry':
                    d['industry'] = element.attrib['resource'].rsplit('#', maxsplit=1)[-1]
                    continue
                elif element.tag == 'organization':
                    d['organization'] = element.attrib['resource'].rsplit('#', maxsplit=1)[-1]
                    continue
                elif element.tag == 'federal_district':
                    d['federal_district'] = element.attrib['resource'].rsplit('#', maxsplit=1)[-1]
                    continue
                elif element.tag == 'identifier':
                    d[element.tag] = element.text 
                    continue
                elif len(list(element)) >= 1: 
                    for sub_element in list(element):
                        if len(list(sub_element)) >= 1:
                            for sub_sub_element in list(sub_element):
                                d[element.tag + '_' + sub_element.tag + '_' + sub_sub_element.tag] = sub_sub_element.text 

                        elif sub_element.text != None:        
                            d[element.tag + '_' + sub_element.tag] = sub_element.text

                elif element.text != None:
                    d[element.tag] = element.text

                else:
                    continue

            elem.clear()
            for ancestor in elem.xpath('ancestor-or-self::*'):
                    while ancestor.getprevious() is not None:
                        del ancestor.getparent()[0]    
                        
            #добавим переменную с хеш-суммой строки         
            md5_hash = create_md5(d, added=self.md5hash_variables) #########################################################################3
            if md5_hash not in old_hash_set:
                    old_hash_set.add(md5_hash)
                    #при необходимости версионирования
                    #new_hash_list.append(md5_hash)
                    #добавим переменную с хеш-суммой строки 
                    d['md5_hash'] = md5_hash
                    l.append(d)  # Добавим этот словарик в список                        
                        
                    if len(l) == self.csv_size: 
                        df = pd.DataFrame(l)

                        #df.to_csv(self.datadir + f'/vacancies{i}.csv', index=False)
                        save_pickle(df, self.datadir + f'/{self.name}{i}.pickle')
                        i = i + 1
                        l = []
                        df = pd.DataFrame()
                
        df = pd.DataFrame(l)
        #df.to_csv(self.datadir + '/vacancies.csv', index=False)
        save_pickle(df, self.datadir + f'/{self.name}.pickle')
        print(f'{self.name} is prepared')
        
# Парсинг файла с откликами
class ParseResponses(Parse):
    def __init__(self):
        Parse.__init__(self, 'responses')
        
    def to_csvs(self):
        df = pd.DataFrame()
        i = 1
        l = []
        old_hash_set = hashes(self.name)

        for event, elem in tqdm(etree.iterparse(self.pathxml, tag='response', recover=True)):
            md5_hash = hashlib.md5(etree.tostring(elem, encoding='UTF-8')).hexdigest()
            if md5_hash not in old_hash_set:
                old_hash_set.add(md5_hash)
                d = {}
                d['date_last_updated'] = self.date
                
                for element in list(elem): 
                    if element.tag == 'link':
                        continue

                    elif len(list(element)) >= 1: 
                        for sub_element in list(element):
                            d[element.tag + '_' + sub_element.tag] = sub_element.text
                            if len(list(sub_element)) >= 1:
                                for sub_sub_element in list(sub_element):
                                    d[element.tag + '_' + sub_element.tag + '_' + sub_sub_element.tag] = sub_sub_element.text 
                    elif element.text != None:
                        d[element.tag] = element.text 

                    else:
                        continue
            
                d['md5_hash'] = md5_hash

                elem.clear()
                for ancestor in elem.xpath('ancestor-or-self::*'):
                        while ancestor.getprevious() is not None:
                            del ancestor.getparent()[0]    
                l.append(d)
                #print(d)
                #break

                if len(l) == self.csv_size: 
                    df = pd.DataFrame(l)
                    #df.to_csv(self.datadir + f'/responses{i}.csv', index=False)
                    save_pickle(df, self.datadir + f'/{self.name}{i}.pickle')
                    i = i + 1
                    l = []
                    df = pd.DataFrame()
                
        df = pd.DataFrame(l) 
        #df.to_csv(self.datadir + '/responses.csv', index=False)
        save_pickle(df, self.datadir + f'/{self.name}.pickle')
        print(f'{self.name} is prepared')

# Парсинг файла с приглашениями
class ParseInvitations(Parse):
    def __init__(self):
        Parse.__init__(self, 'invitations')
        
    def to_csvs(self):
        df = pd.DataFrame()
        i = 1
        l = []
        #add_data = AddingDataPsycopg()
        #old_hash_set = set(add_data.get_hash_list('invitations', 'project_trudvsem'))
        #add_data.conn.close()
        old_hash_set = hashes(self.name)

        for event, elem in tqdm(etree.iterparse(self.pathxml, tag='invitation', recover=True)):
            md5_hash = hashlib.md5(etree.tostring(elem, encoding='UTF-8')).hexdigest()
            if md5_hash not in old_hash_set:
                old_hash_set.add(md5_hash)
                d = {}
                d['date_last_updated'] = self.date
                for element in list(elem): 
                    if element.tag == 'link':
                        continue

                    elif len(list(element)) >= 1: 
                        for sub_element in list(element):
                            d[element.tag + '_' + sub_element.tag] = sub_element.text
                            if len(list(sub_element)) >= 1:
                                for sub_sub_element in list(sub_element):
                                    d[element.tag + '_' + sub_element.tag + '_' + sub_sub_element.tag] = sub_sub_element.text 
                    elif element.text != None:
                        d[element.tag] = element.text 

                    else:
                        continue

                d['md5_hash'] = md5_hash

                elem.clear()
                for ancestor in elem.xpath('ancestor-or-self::*'):
                        while ancestor.getprevious() is not None:
                            del ancestor.getparent()[0]    
                l.append(d)

                if len(l) == self.csv_size: 
                    df = pd.DataFrame(l)

                    #df.to_csv(self.datadir + f'/invitations{i}.csv', index=False) 
                    save_pickle(df, self.datadir + f'/{self.name}{i}.pickle')
                    i = i + 1
                    l = []
                    df = pd.DataFrame()
                
        df = pd.DataFrame(l) 
        #df.to_csv(self.datadir + '/invitations.csv', index=False)
        save_pickle(df, self.datadir + f'/{self.name}.pickle')
        print(f'{self.name} is prepared')
##################################################################################################################################################
# Парсинг файла с организациями
class ParseOrganizations(Parse):
    def __init__(self):
        Parse.__init__(self, 'organizations')
        
    def to_csvs(self):
        df = pd.DataFrame()
        i = 1
        l = []
        new_hash_list = []
        add_data = AddingDataPsycopg()
        old_hash_set = set(add_data.get_hash_list('organizations', 'project_trudvsem'))
        add_data.conn.close()
        for event, elem in tqdm(etree.iterparse(self.pathxml, tag='organization', recover=True)):
            md5_hash = hashlib.md5(etree.tostring(elem, encoding='UTF-8')).hexdigest()
            new_hash_list.append(md5_hash)
            if md5_hash not in old_hash_set:
                d = {}
                d['date_last_updated'] = self.date
                d['id_organization'] = elem.attrib['about'].rsplit('#', maxsplit=1)[-1]
                for element in list(elem): # Проходимся по каждому элементу в наборе
                    if element.tag == 'region':
                        d['region_code'] = element.attrib['resource'].rsplit('#', maxsplit=1)[-1]

                    elif len(list(element)) >= 1: 
                        for sub_element in list(element):
                            d[element.tag + '_' + sub_element.tag] = sub_element.text
                            if len(list(sub_element)) >= 1:
                                for sub_sub_element in list(sub_element):
                                    d[element.tag + '_' + sub_element.tag + '_' + sub_sub_element.tag] = sub_sub_element.text 
                    elif element.text != None:
                        d[element.tag] = element.text  

                    else:
                        continue
                    
                d['md5_hash'] = hashlib.md5(etree.tostring(elem, encoding='UTF-8')).hexdigest()

                elem.clear()
                for ancestor in elem.xpath('ancestor-or-self::*'):
                        while ancestor.getprevious() is not None:
                            del ancestor.getparent()[0]    
                l.append(d)

            if len(l) == self.csv_size: 
                df = pd.DataFrame(l)

                #df.to_csv(self.datadir + f'/organizions{i}.csv', index=False) 
                save_pickle(df, self.datadir + f'/{self.name}{i}.pickle')
                i = i + 1
                l = []
                df = pd.DataFrame()
                
        df = pd.DataFrame(l) 
        #df.to_csv(self.datadir + '/organizions.csv', index=False)
        save_pickle(df, self.datadir + f'/{self.name}.pickle')
        print(f'{self.name} is prepared')
        
# Парсинг файла с регионами
class ParseRegions(Parse):
    def __init__(self):
        Parse.__init__(self, 'regions')
        
    def to_csvs(self):
        df = pd.DataFrame()
        i = 1
        l = []
        code = ['ACCOMODATION_ACCESSIBILITY', 'ATTRACTION_REGION', 'ECONOMIC_GROWTH', 'KINDERGARTEN_ACCESSIBILITY', 
                                'MEDIUM_SALARY_DIFFERENCE', 'PRICE_LEVEL', 'UNEMPLOYMENT_LEVEL']

        for event, elem in tqdm(etree.iterparse(self.pathxml, tag='region', recover=True)):
            d = {}
            #d['date_last_updated'] = self.date
            for element in list(elem): 
                if element.tag == 'link':
                    continue

                elif len(list(element)) >= 1: 
                    ind = 0
                    for sub_element in list(element):
                        if len(list(sub_element)) >= 1: 
                            for sub_sub_element in list(sub_element):
                                if sub_sub_element.tag == 'value':
                                    d[code[ind].lower()] = sub_sub_element.text 
                            ind += 1

                elif element.text != None:
                    d[element.tag] = element.text 

                else:
                    continue
                    
            #d['md5_hash'] = hashlib.md5(etree.tostring(elem, encoding='UTF-8')).hexdigest()

            elem.clear()
            for ancestor in elem.xpath('ancestor-or-self::*'):
                    while ancestor.getprevious() is not None:
                        del ancestor.getparent()[0]    
            l.append(d)

            if len(l) == self.csv_size: 
                df = pd.DataFrame(l)

                #df.to_csv(self.datadir + f'/regions{i}.csv', index=False) 
                save_pickle(df, self.datadir + f'/{self.name}{i}.pickle')
                i = i + 1
                l = []
                df = pd.DataFrame()
                
        df = pd.DataFrame(l) 
        #df.to_csv(self.datadir + '/regions.csv', index=False)
        save_pickle(df, self.datadir + f'/{self.name}.pickle')
        print(f'{self.name} is prepared')
        
# Парсинг файла со сферами деятельности
class ParseIndustries(Parse):
    def __init__(self):
        Parse.__init__(self, 'industries')
        
    def to_csvs(self):
        df = pd.DataFrame()
        i = 1
        l = []

        for event, elem in tqdm(etree.iterparse(self.pathxml, tag='industry', recover=True)):
            d = {}
            #d['date_last_updated'] = self.date
            for element in list(elem): 
                if element.tag == 'link':
                    continue

                elif len(list(element)) >= 1: 
                    for sub_element in list(element):
                        d[element.tag + '_' + sub_element.tag] = sub_element.text
                        
                elif element.text != None:
                    d[element.tag] = element.text 

                else:
                    continue
                    
            #d['md5_hash'] = hashlib.md5(etree.tostring(elem, encoding='UTF-8')).hexdigest()

            elem.clear()
            for ancestor in elem.xpath('ancestor-or-self::*'):
                    while ancestor.getprevious() is not None:
                        del ancestor.getparent()[0]    
            l.append(d)
                
        df = pd.DataFrame(l) 
        #df.to_csv(self.datadir + '/industries.csv', index=False)
        save_pickle(df, self.datadir + f'/{self.name}.pickle')
        print(f'{self.name} is prepared')
        
# Парсинг файла с профессиями
class ParseProfessions(Parse):
    def __init__(self):
        Parse.__init__(self, 'professions')
        
    def to_csvs(self):
        df = pd.DataFrame()
        i = 1
        l = []

        for event, elem in tqdm(etree.iterparse(self.pathxml, tag='prof', recover=True)):
            d = {}
            #d['date_last_updated'] = self.date
            d['profession_code'] = elem.attrib['about'].rsplit('#', maxsplit=1)[-1]
            for element in list(elem): 
                if element.tag == 'link':
                    continue

                elif len(list(element)) >= 1: 
                    for sub_element in list(element):
                        d[element.tag + '_' + sub_element.tag] = sub_element.text
                        
                elif element.text != None:
                    d[element.tag] = element.text 

                else:
                    continue
                    
            #d['md5_hash'] = hashlib.md5(etree.tostring(elem, encoding='UTF-8')).hexdigest()

            elem.clear()
            for ancestor in elem.xpath('ancestor-or-self::*'):
                    while ancestor.getprevious() is not None:
                        del ancestor.getparent()[0]    
            l.append(d)

            if len(l) == self.csv_size: 
                df = pd.DataFrame(l)

                #df.to_csv(self.datadir + f'/professions{i}.csv', index=False) 
                save_pickle(df, self.datadir + f'/{self.name}{i}.pickle')
                i = i + 1
                l = []
                df = pd.DataFrame()
                
        df = pd.DataFrame(l) 
        #df.to_csv(self.datadir + '/professions.csv', index=False)
        save_pickle(df, self.datadir + f'/{self.name}.pickle')
        print(f'{self.name} is prepared')
        
# Парсинг файла со статистикой по гражданам
class ParseStatCitizens(Parse):
    def __init__(self):
        Parse.__init__(self, 'stat_citizens')
        
    def to_csvs(self):
        df = pd.DataFrame()
        i = 1
        l = []
        code = ['cvs_count', 'medium_salary']

        for event, elem in tqdm(etree.iterparse(self.pathxml, tag='region', recover=True)):
            d = {}
            #d['date_last_updated'] = self.date
            for element in list(elem): 
                if element.tag == 'link':
                    continue

                elif len(list(element)) >= 1: 
                    ind = 0
                    for sub_element in list(element):
                        if len(list(sub_element)) >= 1: 
                            for sub_sub_element in list(sub_element):
                                if sub_sub_element.tag == 'value':
                                    d[code[ind].lower()] = sub_sub_element.text 
                            ind += 1

                elif element.text != None:
                    d[element.tag] = element.text 

                else:
                    continue
                    
            #d['md5_hash'] = hashlib.md5(etree.tostring(elem, encoding='UTF-8')).hexdigest()

            elem.clear()
            for ancestor in elem.xpath('ancestor-or-self::*'):
                    while ancestor.getprevious() is not None:
                        del ancestor.getparent()[0]    
            l.append(d)

            if len(l) == self.csv_size: 
                df = pd.DataFrame(l)

                #df.to_csv(self.datadir + f'/stat_citizens{i}.csv', index=False) 
                save_pickle(df, self.datadir + f'/{self.name}{i}.pickle')
                i = i + 1
                l = []
                df = pd.DataFrame()
                
        df = pd.DataFrame(l) 
        #df.to_csv(self.datadir + '/stat_citizens.csv', index=False)
        save_pickle(df, self.datadir + f'/{self.name}.pickle')
        print(f'{self.name} is prepared')
        
# Парсинг файла со статистикой по организациям
class ParseStatCompany(Parse):
    def __init__(self):
        Parse.__init__(self, 'stat_companies')
        
    def to_csvs(self):
        df = pd.DataFrame()
        i = 1
        l = []
        code = ['company_count', 'micro_company', 'small_company', 'midle_company', 'big_company', 'large_company']

        for event, elem in tqdm(etree.iterparse(self.pathxml, tag='region', recover=True)):
            d = {}
            #d['date_last_updated'] = self.date
            d = {}
            for element in list(elem): 
                if element.tag == 'link':
                    continue

                elif len(list(element)) >= 1: 
                    ind = 0
                    for sub_element in list(element):
                        if len(list(sub_element)) >= 1: 
                            for sub_sub_element in list(sub_element):
                                if sub_sub_element.tag == 'value':
                                    d[code[ind].lower()] = sub_sub_element.text 
                            ind += 1

                elif element.text != None:
                    d[element.tag] = element.text 

                else:
                    continue
                    
            #d['md5_hash'] = hashlib.md5(etree.tostring(elem, encoding='UTF-8')).hexdigest()

            elem.clear()
            for ancestor in elem.xpath('ancestor-or-self::*'):
                    while ancestor.getprevious() is not None:
                        del ancestor.getparent()[0]    
            l.append(d)

            if len(l) == self.csv_size: 
                df = pd.DataFrame(l)

                #df.to_csv(self.datadir + f'/stat_company{i}.csv', index=False) 
                save_pickle(df, self.datadir + f'/{self.name}{i}.pickle')
                i = i + 1
                l = []
                df = pd.DataFrame()
                
        df = pd.DataFrame(l) 
        #df.to_csv(self.datadir + '/stat_company.csv', index=False)
        save_pickle(df, self.datadir + f'/{self.name}.pickle')
        print(f'{self.name} is prepared')
        
