
import re
import os
import pandas as pd
from os import listdir
import datetime
from conf import Config

"""Модуль, который инициализирует класс CleaningData
предназначен для реализации процедуры очистки данных и 
преобразования переменных"""


class CleaningData:
    """Класс CleaningData предназначен для реализации процедуры
    очистки данных и преобразования переменных"""
    
    def snake_case_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Изменить названия колонок в датафрейме из CamelCase в snake_case"""
        renamed_columns = []
        pattern = re.compile(r'(?<!^)(?=[A-Z])')
        for name in df.columns.tolist():
            name = pattern.sub('_', name).lower()
            renamed_columns.append(name)
        df.columns = renamed_columns
        return df

    def better_col_names(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Поменять названия переменных на утверждённые"""
        dict_names = Config(os.path.join('./src/', 'all_tables_names.yml')).get_config('better_col_names')
        names = dict_names[table_name]
        for k, v in names.items(): ###
            if v not in df.columns:
                df.rename(columns={k : v}, inplace=True)
        return df  
    
    def str_clean_first(self, string: str) -> str:
        """Return the string with some html tags and artefacts deleted"""
        if type(string) == str:
            string = re.sub(r'style="[A-Za-z0-9!\;\#\s\&\*\+\-\.\^_`\|\~:\,\'\%\(\)]+"', ' ', string)
            string = re.sub(r'class="[A-Za-z0-9!\;\#\s\&\*\+\-\.\^_`\|\~:\,]+"', ' ', string)
            string = re.sub(r'align="[A-Za-z0-9!\;\#\s\&\*\+\-\.\^_`\|\~:\,]+"', ' ', string)
            string = re.sub(r'lang="[A-Za-z0-9!\;\#\s\&\*\+\-\.\^_`\|\~:\,]+"', ' ', string)
            string = re.sub(r'xml:lang="[A-Za-z0-9!\;\#\s\&\*\+\-\.\^_`\|\~:\,]+"', ' ', string)
            string = re.sub(r'id="[A-Za-z0-9!\;\#\s\&\*\+\-\.\^_`\|\~:\,]+"', ' ', string)
            string = re.sub(r'dir="[A-Za-z0-9!\;\#\s\&\*\+\-\.\^_`\|\~:\,]+"', ' ', string)
            string = re.sub(r'data-[A-Za-z0-9\-\s]+="[A-Za-z0-9!\;\#\s\&\*\+\-\.\$\^_`\|\~:\,\{\}]+"', ' ', string)
            string = string.replace('&bull;', ' ')
            string = string.replace('&raquo;', '\" ')
            string = string.replace('&laquo;', '\"')
            string = string.replace('&rdquo;', '\" ')
            string = string.replace('&ldquo;', '\"')
            string = string.replace('&rsquo;', '\" ')
            string = string.replace('&lsquo;', '\"')
            string = string.replace('&ndash;', '- ')
            string = string.replace('&mdash;', '- ')
            string = string.replace('&nbsp;', ' ')
            string = string.replace('&sbquo;', ' ')
            string = string.replace('&middot;', '- ')
            string = string.replace('&minus;', '- ')
            string = string.replace('&shy;', ' ')
            string = string.replace('laquo;', '\"')
            string = string.replace('raquo;', '\" ')
            string = string.replace('ndash;', '- ')
            string = string.replace('mdash;', '- ')
            string = string.replace('bull;', ' ')
            string = string.replace('rdquo;', '\" ')
            string = string.replace('ldquo;', '\"')
            string = string.replace('rsquo;', '\" ')
            string = string.replace('lsquo;', '\"')
            string = string.replace('nbsp;', ' ')
            string = string.replace('sbquo;', ' ')
            string = string.replace('middot;', '- ')
            string = string.replace('minus;', '- ')
            string = string.replace('shy;', ' ')
            string = string.replace('&lt;', ' ')
            string = string.replace('&gt;', ' ')
            string = string.lstrip('p')
            string = string.replace('/pp', ' ')
            string = string.replace('</p><p', '| ')
            string = string.replace('</li><li', '| ')
            string = string.replace('<p', ' ')
            string = string.strip()
            string += '.' 
            return string
        else:
            return None

    def get_change_urls(self, string: str) -> str:
        """Функция собирающая URLs и меняющая их на порядковый номер,
        возвращает изменённую строку и список URLs"""
        if type(string) == str:
            url = re.compile(r'(?i)(\b(?:(?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4})(?:[^\s()<>]+))|\
            english|portfolio|library|management|academy|simens|scholarship|australia|consult|college|handling|\
            anklin|ember|california|school|discipline)+')

            l_urls = url.findall(string)
            for i, u in enumerate(l_urls):
                string = string.replace(u, 'URLno' + str(i))
            return string, l_urls
        else:
            return string, None

    def str_clean_without_urls(self, string: str) -> str:
        """Return the string with html tags replace by space
        """    
        if type(string) == str:
            string = string.replace('/span', ' ')
            string = string.replace('/li', ' ')
            string = string.replace('/ol', ' ')
            string = string.replace('/ul', ' ')
            string = string.replace('/em', ' ')
            string = string.replace('/strong', ' ')
            string = string.replace('/p', ' ')
            string = string.replace('span', ' ')
            string = string.replace('strong', ' ')
            string = string.replace('li', ' ')
            string = string.replace('ol', ' ')
            string = string.replace('ul', ' ')
            string = string.replace('em', ' ')
            string = string.replace('br', ' ')
            string = string.replace('title=', ' ')
            return string
        else:
            return None

    def back_urls(self, string: str, list_urls: list) -> str:
        """Функция для возврата URLов в строку"""
        if type(string) == str:
            for i in range(len(list_urls)):
                string = string.replace('URLno'+str(i), list_urls[i])
            return string
        else:
            return None

    def str_clean_last(self, string: str) -> str:
        """Return the string with ';'s replaced by '|'s,
        punctuation marks cleaned and
        multiple spaces replaced by single space
        """
        if type(string) == str:
            string = string.replace('<', ' ')
            string = string.replace('>', ' ')
            string = string.replace(';', '|')
            string = string.replace(' :', ': ')
            string = re.sub(r'[\|]+', '|', string)
            string = re.sub(r'[\s]+', ' ', string)
            string = string.replace(' ,', ',')
            string = string.strip(' p.')
            return string
        else:
            return None

    def clean_out_html(self, series: pd.Series) -> pd.Series:
        """Функция для обработки пандас-серии пятью
        вышеописанными последовательными функциями"""
        first = series.apply(self.str_clean_first)
        second = first.apply(self.get_change_urls)
        temp_df = pd.DataFrame(second.tolist(),
                             index=second.index,
                             columns = ['strings', 'urls'])
        temp_df['strings'] = temp_df.strings.apply(self.str_clean_without_urls)
        temp_df['strings_with_urls'] = temp_df.apply(lambda x: self.back_urls(x.strings, x.urls), axis=1)
        temp_df['strings_with_urls'] = temp_df.strings_with_urls.apply(self.str_clean_last)
        return temp_df['strings_with_urls']

    def del_punct_dig(self, string: str) -> str:
        """Функция удаляет строки, состоящие только из одного символа,
        либо из знаков пунктуации, пробелов и цифр"""
        if type(string) == str:
            pattern = re.compile(r"^$|.|(\W|\_|\d)+")
            string_matches = pattern.fullmatch(string)
            if string_matches:
                string = None
            return string
        else:
            return None
        
    def del_punct(self, string: str) -> str:
        """Функция удаляет строки, состоящие только из одного символа,
        либо из знаков пунктуации и пробелов"""
        if type(string) == str:
            pattern = re.compile(r"^$|.|(\W|\_)+")
            string_matches = pattern.fullmatch(string)
            if string_matches:
                string = None
            return string
        else:
            return None

    def dummies(self, series_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """Функция для создания dummy-переменных по каждой категории
        водительских прав, типу графика работы, типу категории 
        социальной защиты, преимущества вакансии"""
        dumm_df = pd.DataFrame()
        if series_name == 'drive_licences':
            dumm_df = df.drive_licences.str.strip('[]').str.replace(', ', "|").str.get_dummies()
            dumm_df = dumm_df.add_prefix('driver_licence_')
        elif series_name == 'schedule_type':
            dumm_df = df.schedule_type.str.replace('Вахтовый метод', "1").str.replace('Гибкий график', "2") \
                .str.replace('Ненормированный рабочий день', "3").str.replace('Неполный рабочий день', "4") \
                .str.replace('Полный рабочий день', "5").str.replace('Сменный график', "6") \
                .str.replace(',', "|").str.get_dummies()
            dumm_df = dumm_df.add_prefix('schedule_type_')
        elif series_name == 'social_protecteds_social_protected':
            list_dict = {
                'Инвалиды': 'disabled',
                'Многодетные семьи': 'large_families',
                'Работники; имеющие детей-инвалидов': 'workers_with_disabled_children',
                'Работники; осуществляющие уход за больными членами их семей в соответствии с медицинским заключением': 'caring_workers',
                'Матери и отцы; воспитывающие без супруга (супруги) детей в возрасте до пяти лет': 'single_parent',
                'Лица; освобождаемые из мест лишения свободы': 'released_persons',
                'Несовершеннолетние работники': 'minor_workers'}
            dumm_df = pd.DataFrame(columns = [key for key in list_dict.keys()])
            dumm_df = df['social_protecteds_social_protected'].str.replace(', ', '; ').str.replace(',', '|').str.get_dummies()
            dumm_df.rename(list_dict, axis=1, inplace=True)
        elif series_name == 'job_benefits':
            list_dict = {
                'ДМС': 'dms',
                'Путевки в оздоровительные учреждения': 'vouchers_health_institutions', 
                'Оплата занятий спортом': 'payment_sports_activities',
                'Оплата питания': 'payment_meals'}
            dumm_df = pd.DataFrame(columns = [key for key in list_dict.keys()])
            dumm_df = df['job_benefits'].str.replace(', ', '; ').str.replace(',', '|').str.get_dummies()
            dumm_df.rename(list_dict, axis=1, inplace=True)
        return df.join(dumm_df)

    def create_mistake_col(self, series_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """Функция для оценки и создания колонки
        с флагом ошибочных значений"""
        now = datetime.datetime.now()
        if series_name == 'experience':
            exp_years = df.experience.fillna(1).astype('int').unique().tolist()
            experience_mistake = pd.Series(0, index = df.experience.index)
            wrong_years = [str(x) for x in exp_years if ((x < 0) or (x > 65))]
            experience_mistake[df.experience.isin(wrong_years)] = 1
            return df.insert(df.shape[1], 'experience_mistake', experience_mistake)
        elif series_name == 'birthday':
            birth_years = df.birthday.fillna(1990).astype('int').unique().tolist() ###
            birthday_mistake = pd.Series(0, index = df.birthday.index)
            wrong_years = [str(x) for x in birth_years if ((x < (now.year - 85)) or (x > (now.year - 14)))]
            birthday_mistake[df.birthday.isin(wrong_years)] = 1
            return df.insert(df.shape[1], 'birthday_mistake', birthday_mistake)
        elif series_name == 'graduate_year':
            grad_years = df.loc[df.graduate_year.notna(), 'graduate_year'].astype('int').unique().tolist()
            df['grad_year_mistake'] = 0
            wrong_years = [str(x) for x in grad_years if ((x < (now.year - 70)) or (x > (now.year + 4)))]
            df.loc[df.graduate_year.isin(wrong_years), 'grad_year_mistake'] = 1
            return df

    def exp_dates_mistake(self, df: pd.DataFrame) -> pd.DataFrame:
        """Функция для оценки и создания колонки с флагом ошибочных значений 
        дат начала и окончания трудовой деятельности"""
        now = datetime.datetime.now()
        df['date_mistake'] = 0
        df.loc[df.date_from.str[0:4].fillna(1960).astype('int64') < (now.year - 70), 'date_mistake'] = 1
        df.loc[df.date_to.str[0:4].fillna(1960).astype('int64') < (now.year - 70), 'date_mistake'] = 1
        df.loc[df.date_from.str[0:4].fillna(2021).astype('int64') > now.year, 'date_mistake'] = 1
        df.loc[df.date_to.str[0:4].fillna(2021).astype('int64') > now.year, 'date_mistake'] = 1
        df.loc[df.date_from == df.date_to, 'date_mistake'] = 1
        df.loc[df.date_from > df.date_to, 'date_mistake'] = 1
        return df

    def capital_lower_strip(self, string: str) -> str:
        """Изменим регистр всех слов на нижний,
        первую букву(символ) - в верхний"""
        if type(string) == str:
            return string.strip(' \"\'.,?!«»').lower().capitalize()
        else:
            return None
        
    def time_date_separation(self, col_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """Разделение даты на переменные date_ и time_ с переименованием исходной
        переменной в date_time_"""
        time_col_name = 'time_' + '_'.join(col_name.split('_')[1:])
        date_time_col_name = 'date_time_' + '_'.join(col_name.split('_')[1:])
        df[date_time_col_name] = pd.to_datetime(df[col_name])
        df[time_col_name] = pd.to_datetime(df[date_time_col_name], utc=True).dt.time
        df[col_name] = pd.to_datetime(df[date_time_col_name], utc=True).dt.date
        return df

    def unix_time_mistake(self, df: pd.DataFrame, date_with_year: str) -> pd.DataFrame:
        """Проверка дат на unix time error (01.01.1970),
        если в поле ошибка создается колонка mistake"""
        if pd.DatetimeIndex(df[date_with_year]).year.min() == 1970:
            df[f'{date_with_year}_mistake'] = 0
            df.loc[pd.DatetimeIndex(df[date_with_year]).year == 1970, f'{date_with_year}_mistake'] = 1
            return df
        else:
            print('Unix time error not found')
            
    def region_code_mistake(self, df: pd.DataFrame, date_with_year: str) -> pd.DataFrame:   
        """Проверка кода региона на длину (частная ошибка таблицы,
        если в поле ошибка создается колонка mistake"""
        df[f'{date_with_year}_mistake'] = 0
        df.loc[df[date_with_year].str.len() != 13, f'{date_with_year}_mistake'] = 1
        return df
    
    def abs_col(self, col_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """Приведение значений в колонке к модулю"""
        df.loc[df[col_name].fillna(99887766).astype('int64') < 0, col_name] = \
        df[df[col_name].fillna(99887766).astype('int64') < 0][col_name].astype('int64').apply(abs).astype(str) 
        return df
    
    def description_col(self, df: pd.DataFrame) -> pd.DataFrame:
        """Создание колонки с описанием законченных курсов
        (в случае слишком многословного заполнения полей course_name, legal_name)"""
        df['description'] = None
        if 'course_name' in df.columns and 'legal_name' in df.columns:
            df.loc[df.course_name.str.len() > 200, 'description'] = df.course_name
            df.loc[df.legal_name.str.len() > 350, 'description'] = df.legal_name
            df.loc[(df.course_name.str.len() > 200) & \
                (df.legal_name.str.len() > 350) & \
                (df.course_name != df.legal_name), 'description'] = \
            df.course_name.str.cat(df.legal_name, sep=' | ', na_rep='-')
            df.loc[df.course_name.str.len() > 200, 'course_name'] = ' '
            df.loc[df.legal_name.str.len() > 350, 'legal_name'] = ' '
            df.loc[df.course_name.isna(), 'course_name'] = 'Название курса не было указано'
            df.loc[df.legal_name.isna(), 'legal_name'] = 'Название организации не было указано'
        return df
    
    def create_zero_one_col(self, series_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """Функция для создания колонки с переменными в формате 1 или 0"""
        if series_name == 'accommodation_capability':
            df.loc[(df.accommodation_capability == 'жилье предоставляется') | (df.accommodation_capability == 'true'), 'accommodation_capability'] = 1
            df.loc[(df.accommodation_capability == 'жилье не предоставляется') | (df.accommodation_capability == 'false'), 'accommodation_capability'] = 0
            return df
        elif series_name == 'inner_info_visibility':
            df.loc[df.inner_info_visibility == 'Видно всем', 'inner_info_visibility'] = 1
            return df
        elif series_name == 'need_medcard':
            df.loc[(df.need_medcard == 'требуется') | (df.need_medcard == 'true'), 'need_medcard'] = 1
            df.loc[(df.need_medcard == 'не требуется') | (df.need_medcard == 'false'), 'need_medcard'] = 0
            return df
        elif series_name == 'retraining_capability':
            df.loc[(df.retraining_capability == 'Готов к переобучению') | (df.retraining_capability == 'true'), 'retraining_capability'] = 1
            df.loc[(df.retraining_capability == 'Не готов к переобучению') | (df.retraining_capability == 'false'), 'retraining_capability'] = 0
            return df
        elif series_name == 'retraining_grant':
            df.loc[(df.retraining_grant == 'есть стипендия') | (df.retraining_grant == 'true'), 'retraining_grant'] = 1
            df.loc[(df.retraining_grant == 'нет стипендии') | (df.retraining_grant == 'false'), 'retraining_grant'] = 0
            return df
        elif series_name == 'activity_flag_candidate':
            df.loc[(df.activity_flag_candidate == 'Активен') | (df.activity_flag_candidate == 'true'), 'activity_flag_candidate'] = 1
            df.loc[(df.activity_flag_candidate == 'Не активен') | (df.activity_flag_candidate == 'false'), 'activity_flag_candidate'] = 0
            return df
        elif series_name == 'activity_flag_manager':
            df.loc[(df.activity_flag_manager == 'Активен') | (df.activity_flag_manager == 'true'), 'activity_flag_manager'] = 1
            df.loc[(df.activity_flag_manager == 'Не активен') | (df.activity_flag_manager == 'false'), 'activity_flag_manager'] = 0
            return df
        elif series_name == 'is_new':
            df.loc[df.is_new == 'Новый отклик', 'is_new'] = 1
            df.loc[df.is_new == 'Не новый отклик', 'is_new'] = 0
            return df
        elif series_name == 'first_rate_company':
            df.loc[(df.first_rate_company == 'Относится к крупнейшим компаниям') | (df.first_rate_company == 'true'), 'first_rate_company'] = 1
            df.loc[(df.first_rate_company == 'Не относится к крупнейшим компаниям') | (df.first_rate_company == 'false'), 'first_rate_company'] = 0
            return df
        elif series_name == 'abilympics_participation':
            df.loc[(df.abilympics_participation == 'Участник движения') | (df.abilympics_participation == 'Принимал участие в движении Абилимпикс'), 'abilympics_participation'] = 1
            df.loc[(df.abilympics_participation == 'Не принимал участие в движении Абилимпикс'), 'abilympics_participation'] = 0
            return df
        elif series_name == 'business_trips':
            df.loc[(df.business_trips == 'Готов к командировкам') | (df.business_trips == 'true'), 'business_trips'] = 1
            df.loc[(df.business_trips == 'Не готов к командировкам') | (df.business_trips == 'false'), 'business_trips'] = 0
            return df
        elif series_name == 'nark_certificate':
            df.loc[df.nark_certificate == 'Свидетельство о независимой оценке квалификации', 'nark_certificate'] = 1
            return df
        elif series_name == 'nark_inspection_status':
            df.loc[df.nark_inspection_status == 'Данные подтверждены', 'nark_inspection_status'] = 1
            return df
        elif series_name == 'relocation':
            df.loc[(df.relocation == 'Да') | (df.relocation == 'true') | (df.relocation == "Готов к переезду"), 'relocation'] = 1
            df.loc[(df.relocation == 'Нет') | (df.relocation == 'false') | (df.relocation == "Не готов к переезду"), 'relocation'] = 0
            return df
        elif series_name == 'worldskills_type':
            df.loc[df.worldskills_type == 'Participation', 'worldskills_type'] = 1
            return df
    
    def del_not_dig(self, string: str) -> str:
        """Функция удаляет строки, состоящие не из цифр"""
        if type(string) == str:
            pattern = re.compile(r"\d+")
            string_matches = pattern.fullmatch(string)
            if string_matches:
                return string
            else:
                return None

#
