import pandas as pd
import unify_cols
from  cleaning_class import CleaningData
cleaning = CleaningData()

def process(table_name, df):
        # общие для всех таблиц: приведение названий колонок в правильный вид
    cleaning.snake_case_names(df)
    cleaning.better_col_names(df, table_name)
    
    # обработка резюме
    if table_name == 'curricula_vitae':
        cols = ['add_certificates_orig', 
                'additional_skills_orig', 
                'other_info_orig', 
                'skills_orig']
                
        for col_name in cols:
            if col_name in df.columns:
                new_col_name = '_'.join(col_name.split('_')[:-1])
                df[new_col_name] = cleaning.clean_out_html(df[col_name])
                df[new_col_name] = df[new_col_name].apply(cleaning.del_punct_dig)
        try:
            df['add_certificates_modified'] = df.add_certificates.apply(rostrud_ml.process.unify_cols.certificate)
            df['other_info_modified'] = df.other_info.apply(rostrud_ml.process.unify_cols.other_info)
        except Exception as e:
            print(e)
            
        
        onehot_cols = ['drive_licences', 
                       'schedule_type']
        for col in onehot_cols:
            if col_name in df.columns:
                df = cleaning.dummies(col, df)
        
        mistake_cols = ['birthday', 
                        'experience']
        for col in mistake_cols:
            if col in df.columns:
                cleaning.create_mistake_col(col, df)
                
        cols = ['abilympics_participation', 'business_trips', 'nark_certificate',
                'nark_inspection_status', 'relocation', 'worldskills_type', 
                'retraining_capability', 'inner_info_visibility']
        for col in cols:
            if col in df.columns:
                cleaning.create_zero_one_col(col, df)
        if 'position_name_orig' in df.columns:
            df['position_name'] = df.position_name_orig.apply(cleaning.del_punct_dig)
            df.position_name = df.position_name.apply(cleaning.capital_lower_strip)
        if 'salary' in df.columns:
            df = cleaning.abs_col('salary', df)
        if 'date_publish' in df.columns:
            df = cleaning.time_date_separation('date_publish', df)
        df['inactive'] = 0
        df['profession_code'] = df['profession_code'].apply(cleaning.del_not_dig)
    
    # обработка опыта работы
    if table_name == 'workexp':
        # удаляем дубликаты строк
        df.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=True)
        cols = ['achievements_orig', 
                'demands_orig']
        for col_name in cols:
            if col_name in df.columns:
                new_col_name = col_name.rsplit('_', maxsplit=1)[0]
                df[new_col_name] = cleaning.clean_out_html(df[col_name])
                df[new_col_name] = df[new_col_name].apply(cleaning.del_punct_dig)
        try:
            df['achievements_modified'] = df.achievements.apply(rostrud_ml.process.unify_cols.achievements)
        except Exception as e:
            print(e)
            
        df['job_title'] = df.job_title_orig.apply(cleaning.del_punct_dig)
        df.job_title = df.job_title.apply(cleaning.capital_lower_strip)
        
        df['company_name'] = df.company_name.str.replace('000', 'ООО', n=1)
        df['company_name'] = df.company_name.apply(cleaning.del_punct_dig)
        
        df = cleaning.exp_dates_mistake(df)
        
    # обработка образования
    if table_name == 'edu':
        # удаляем дубликаты строк
        # print(df.columns)
        
        df.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=True)
        df = cleaning.create_mistake_col('graduate_year', df)
        
        cols = ['faculty_orig', 
                'legal_name_orig',
                'qualification_orig', 
                'speciality_orig']
        for col_name in cols:
            if col_name in df.columns:
                new_col_name = col_name.rsplit('_', maxsplit=1)[0]
                df[new_col_name] = df[col_name].apply(cleaning.del_punct)
        
        df.faculty = df.faculty.apply(cleaning.capital_lower_strip)
        
    # обработка дополнительного образования
    if table_name == 'addedu':
        # удаляем дубликаты строк
        df.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=True)
        df = cleaning.create_mistake_col('graduate_year', df)
        
        cols = ['course_name_orig', 
                'legal_name_orig']
        for col_name in cols:
            if col_name in df.columns:
                new_col_name = col_name.rsplit('_', maxsplit=1)[0]
                df[new_col_name] = df[col_name].apply(cleaning.del_punct_dig)
            
        df = cleaning.description_col(df)
        
    # обработка приглашений
    if table_name == 'invitations':
        cols = ['activity_flag_candidate', 'activity_flag_manager', 'is_new']
        for col in cols:
            if col in df.columns:
                cleaning.create_zero_one_col(col, df)
        dates_with_year = ['date_creation', 'date_modify']
        for date_with_year in dates_with_year:
            cleaning.unix_time_mistake(df, date_with_year)
    
    # обработка вакансий
    if table_name == 'vacancies':
        cols = ['accommodation_capability', 'inner_info_visibility', 'need_medcard',
                'retraining_capability', 'retraining_grant']
        for col in cols:
            if col in df.columns:
                cleaning.create_zero_one_col(col, df)
        date_cols = ['date_change_inner_info', 'date_posted']
        for col in date_cols:
            try:
                cleaning.time_date_separation(col, df)
            except IndexError:
                continue
            except KeyError:
                continue
        onehot_cols = ['social_protecteds_social_protected', 'drive_licences', 'job_benefits']
        for col in onehot_cols:
            try:
                df = cleaning.dummies(col, df)
            except IndexError:
                continue
            except KeyError:
                continue
        dates_with_year = ['date_creation', 'date_posted', 'date_modify_inner_info', 'date_change_inner_info']
        for date_with_year in dates_with_year:
            try:
                cleaning.unix_time_mistake(df, date_with_year)
            except IndexError:
                continue
            except KeyError:
                continue
        cols = ['base_salary_max', 'base_salary_min']
        for col_name in cols:
            try:
                cleaning.abs_col(col_name, df)
            except IndexError:
                continue
            except KeyError:
                continue
        cols = ['job_benefits_other_benefits_orig', 'additional_info_orig',
        'education_requirements_speciality_orig', 'career_perspective_orig',
        'retraining_condition_orig', 'responsibilities_orig',
        'requirements_qualifications_orig',
        'requirements_required_certificates_orig']
        for col_name in cols:
            if col_name in df.columns:
                try:
                    new_col_name = '_'.join(col_name.split('_')[:-1])
                    df[new_col_name] = cleaning.clean_out_html(df[col_name])
                    df[new_col_name] = df[new_col_name].apply(cleaning.del_punct_dig)
                except IndexError:
                    continue
                except KeyError:
                    continue
        df['inactive'] = 0
            
    # обработка откликов
    if table_name == 'responses':
        cols = ['activity_flag_candidate', 'activity_flag_manager', 'is_new']
        for col in cols:
            if col in df.columns:
                cleaning.create_zero_one_col(col, df)
        dates_with_year = ['date_creation', 'date_modify']
        for date_with_year in dates_with_year:
            try:
                cleaning.unix_time_mistake(df, date_with_year)
            except IndexError:
                continue
            except KeyError:
                continue
                
    # обработка справочника сфер деятельности
    if table_name == 'industries':
        dates_with_year = ['date_creation', 'date_modify']
        for date_with_year in dates_with_year:
            try:
                cleaning.unix_time_mistake(df, date_with_year)
            except IndexError:
                continue
            except KeyError:
                continue
                
    # обработка статистики соискателей
    if table_name == 'stat_citizens':
        cleaning.region_code_mistake(df, 'region_code')
                
    # обработка справочника профессий
    if table_name == 'professions':
        dates_with_year = ['date_creation', 'date_modify']
        for date_with_year in dates_with_year:
            try:
                cleaning.unix_time_mistake(df, date_with_year)
            except IndexError:
                continue
            except KeyError:
                continue
            
    # обработка справочника организаций
    if table_name == 'organizations':
        cols = ['first_rate_company']
        for col in cols:
            if col in df.columns:
                cleaning.create_zero_one_col(col, df)
        date_cols = ['date_change_inner_info']
        for col in date_cols:
            try:
                cleaning.time_date_separation(col, df)
            except IndexError:
                continue
            except KeyError:
                continue
        dates_with_year = ['date_creation', 'date_modify_inner_info', 'date_moderation_inner_info', 'date_change_inner_info']
        for date_with_year in dates_with_year:
            try:
                cleaning.unix_time_mistake(df, date_with_year)
            except IndexError:
                continue
            except KeyError:
                continue
        cols = ['description_orig', 'inner_info_moderation_comment_orig']  
        for col_name in cols:
            if col_name in df.columns:
                try:
                    new_col_name = '_'.join(col_name.split('_')[:-1])
                    df[new_col_name] = cleaning.clean_out_html(df[col_name])
                    df[new_col_name] = df[new_col_name].apply(cleaning.del_punct_dig)
                except IndexError:
                    continue
                except KeyError:
                    continue
    
#     print(table_name, '- готов')
    return df
