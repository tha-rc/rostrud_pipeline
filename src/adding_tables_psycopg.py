
import os#, sys
#sys.path.append('./process')

import pandas as pd
import psycopg2
import tempfile
from conf import Config

"""Модуль, который инициализирует класс AddingDataPsycopg,
предназначен для реализации процедуры выгрузки и добавления
и удаления для нового набора данных"""


class AddingDataPsycopg:
    """Класс AddingDataPsycopg предназначен для реализации процедуры выгрузки, 
    добавления, удаления набора данных"""
    def __init__(self):
        attributes = Config(os.path.join('./src/', 'config_to_bd.yml')).get_config('connection_from')
        self.conn = psycopg2.connect(f"""
        host={attributes['host_from']}
        port={int(attributes['port_from'])}

        dbname={attributes['database_from']}
        user={attributes['user_from']}
        password={attributes['password_from']}
        target_session_attrs=read-write
        """)        #sslmode={attributes['ssl_mode_from']}
        with self.conn.cursor() as cursor:
            cursor.execute('SELECT current_database ();')
            print(f'request to database: {cursor.fetchone()[0]}')
        
    def write_to_sql(self, df: pd.DataFrame, table_name: str, schema: str):
        """Функция, которая которая загружает записи в БД"""
        columns = ', '.join(df.columns.tolist())
        copy_sql = f"""
                  COPY {schema}.{table_name} ({columns}) FROM STDIN WITH CSV HEADER
                  DELIMITER as ','
                  """
        with tempfile.TemporaryFile() as temp:
            temp.write(df.to_csv(index=False).encode())
            temp.seek(0)
            with self.conn.cursor() as cursor:
                cursor.copy_expert(copy_sql, temp)
        self.conn.commit()
    
    def create_table(self, table_name: str, schema: str):
        """Функция, которая создает таблицу в БД"""
        variable_list = Config(os.path.join('./src/', 'all_tables_names.yml')).get_config('create_table')
        variable = variable_list[table_name]
        with self.conn.cursor() as cursor:
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({variable});")
            cursor.execute("COMMIT")
            
    def rename_table(self, table_name_in: str, table_name_out: str, schema: str):
        """Функция, которая переименовывает таблицу в БД"""
        with self.conn.cursor() as cursor:
            cursor.execute(f"ALTER TABLE {schema}.{table_name_in} RENAME TO {table_name_out};")
            cursor.execute("COMMIT")
    
    def delete_table(self, table_name: str, schema: str):
        """Функция, которая удаляет таблицу из БД"""
        with self.conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE {schema}.{table_name};")
            cursor.execute("COMMIT")

    def get_table_as_df(self, columns_name: str, table_name: str, schema: str) -> pd.DataFrame:
        """Функция, которая выгружает данные из таблицы
        (простой вариант для одной таблицы)"""
        with tempfile.TemporaryFile() as tmpfile:
            copy_sql = f"""COPY (SELECT {columns_name} FROM {schema}.{table_name}) TO STDOUT WITH CSV HEADER"""
            with self.conn.cursor() as cursor:
                cursor.copy_expert(copy_sql, tmpfile)
            tmpfile.seek(0)
            df = pd.read_csv(tmpfile)
        return df
    
    def query_exec_as_df(self, query: str) -> pd.DataFrame:
        """Функция, которая выгружает данные из таблицы
        (простой вариант для одной таблицы)"""
        with tempfile.TemporaryFile() as tmpfile:
            copy_sql = f"""COPY ({query}) TO STDOUT WITH CSV HEADER"""
            with self.conn.cursor() as cursor:
                cursor.copy_expert(copy_sql, tmpfile)
            tmpfile.seek(0)
            df = pd.read_csv(tmpfile)
        return df
    
    def get_table_as_df_join(self, query_name: str, key: str) -> pd.DataFrame:
        """Функция, которая выгружает данные из таблицы по запросу из словаря"""
        query_list = Config(os.path.join('./src/', 'all_tables_names.yml')).get_config(key)
        query = query_list[query_name]
        with tempfile.TemporaryFile() as tmpfile:
            copy_sql = f"""COPY ({query}) TO STDOUT WITH CSV HEADER"""
            with self.conn.cursor() as cursor:
                cursor.copy_expert(copy_sql, tmpfile)
            tmpfile.seek(0)
            df = pd.read_csv(tmpfile)
        return df
    
    def delete_strings(self, id_from: str, id_in: str, table_name_from: str, table_name_in: str, schema: str):
        """Функция, которая удаляет записи из таблицы лемм если ее
        нет в таблице"""
        with self.conn.cursor() as cursor:
            cursor.execute(f"""DELETE
            FROM {schema}.{table_name_from}
            WHERE NOT EXISTS (SELECT {id_in} FROM {schema}.{table_name_in}
            WHERE {schema}.{table_name_from}.{id_from} = {schema}.{table_name_in}.{id_in})""")
            cursor.execute("COMMIT")
            print('Строки удалены')

    def get_hash_list(self, table_name: str, schema: str) -> list:
        """Функция, которая собирает хеш-суммы"""
        with tempfile.TemporaryFile() as tmpfile:
            copy_sql = f"""COPY (SELECT md5_hash FROM {schema}.{table_name}) TO STDOUT CSV"""
            with self.conn.cursor() as cursor:
                cursor.copy_expert(copy_sql, tmpfile)
            tmpfile.seek(0)
            #тк последняя строка заканчивается на \n последний по сплиту элемент - пустая строка, её не берём
            results = tmpfile.read().decode().split('\n')[:-1] 
        return results
    
    def get_column_list(self, columns_name: str, table_name: str, schema: str) -> str:
        """Функция, которая собирает переменную в список"""
        with tempfile.TemporaryFile() as tmpfile:
            copy_sql = f"""COPY (SELECT {columns_name} FROM {schema}.{table_name}) TO STDOUT CSV"""
            with self.conn.cursor() as cursor:
                cursor.copy_expert(copy_sql, tmpfile)
            tmpfile.seek(0)
            #тк последняя строка заканчивается на \n последний по сплиту элемент - пустая строка, её не берём
            results = tmpfile.read().decode().split('\n')[:-1] 
        return results

    def add_column(self, table_name: str, schema: str, column: str):
        """Функция, которая добавляет колонку в существующую таблицу"""
        with self.conn.cursor() as cursor:
            cursor.execute(f"""ALTER TABLE {schema}.{table_name}
                            ADD COLUMN {column};""")
            cursor.execute("COMMIT")

#
