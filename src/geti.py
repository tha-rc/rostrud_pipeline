import os
import pickle
import datetime
import hashlib
from adding_tables_psycopg import AddingDataPsycopg
from conf import Config


def hashes(table_name):
    workdir = Config(os.path.join('./src/', 'all_tables_names.yml')).get_config('working_directory')
    hash_path = os.path.join(workdir, table_name + '_hash_list.pickle')
    hash_set = set()

    if os.path.exists(hash_path) and os.path.getsize(hash_path) > 0:
        with open(hash_path, "rb") as fp:   # Unpickling
            hash_set = pickle.load(fp)
    else:
        add_data = AddingDataPsycopg()
        hash_set = set(add_data.get_hash_list(table_name, 'project_trudvsem'))
        add_data.conn.close()
    print(f'hashes of {table_name} are returned: {len(hash_set)}')
    return hash_set

def update_hashes(table, old_hash_set, new_hash_list):
    print(datetime.datetime.now().time(), 'renew pickled hashes')
    # запишем обновлённый список хешей в наш кэш-файл
    all_hashes = set(new_hash_list).union(old_hash_set)
    workdir = Config(os.path.join('./src/', 'all_tables_names.yml')).get_config('working_directory')
    hash_path = os.path.join(workdir, table + '_hash_list.pickle')
    with open(hash_path, "wb") as fp:   #Pickling
        pickle.dump(all_hashes, fp)
    print('hashes saved')
    
def create_md5(row, added=None, excluded=['date_last_updated', 'innerInfo_dateModify', 'publishDate']):
    if added == None: ##
        added = row.keys() ##
    row = {x: y for x, y in sorted(row.items()) if x in added and x not in excluded}
    return hashlib.md5(''.join(row.values()).encode()).hexdigest()
