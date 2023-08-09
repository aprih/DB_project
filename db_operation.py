import pandas as pd
import os
from os import walk
import string
import dask
import csv
import chardet
from sqlalchemy import create_engine
from collections import deque
import shutil

# функция, которая выводит n последних строк в файле
def tail(filename, n=1):
    'Return the last n lines of a file'
    with open(filename) as f:
        return deque(f, n)

# функция для записи в файл строки
def write(filename, string):
  file = open(filename, 'a')
  file.write("\n" + string)

# функция, которая задает параметры базы данных
def get_params_db():
    
    user = 'postgres'
    password = 'secret_228'
    db_driver = 'org.postgresql.Driver'
    host = 'localhost'
    dbname = 'postgres'
    db_url_sp = f'jdbc:postgresql://{host}:5432/{dbname}?user={user}&password={password}'
    db_url_alc = f'postgresql://{user}:{password}@{host}:5432/{dbname}'
    
    return user, password, db_driver, host, dbname, db_url_sp, db_url_alc

# функция, которая нужна для передачи параметров подключения БД
def get_engine(params = get_params_db()):
  return create_engine(params[-1])

# функция, которая читает таблицу в датафрейм
def read_table(table_name, schema, params = get_params_db()):
    tab = pd.read_sql_table(table_name, schema = schema, con = params[-1])
    return tab

# фукнция, которая выводит список таблиц в указанной схеме базы данных
def get_df_tables(schema, params = get_params_db()):
    df_tables = pd.read_sql("SELECT * FROM information_schema.tables", con = params[-1])
    return df_tables[df_tables.table_schema == schema].reset_index(drop=True)

# функция, которая сохраняет таблицу в БД
def save_table_to_db(schema, table_name, df, params = get_params_db()):
    
    cnt = 0
    
    tables_df = get_df_tables(schema = schema, params = get_params_db())
    lst_of_tables = list(tables_df.table_name.values)
    lst_of_schemas = list(tables_df.table_schema.values)
    
    for i in range(len(lst_of_tables)):
        
        if (lst_of_tables[i] == table_name) & (lst_of_schemas[i] == schema):
            cnt += 1
            
    if cnt == 0:
      engine = create_engine(params[-1])
      df.to_sql(table_name, engine, index = False, schema = schema)

# функция выводит самую свежую папку в директории
def latest_subdir_of(p = '.'):
  result = []
  for d in os.listdir(p):
    bd = os.path.join(p, d)
    if os.path.isdir(bd): result.append(bd)
  return max(result, key=os.path.getmtime)

# функция, которая очищает содержимое папки
def delete_all_in_dir(dir):
  for d in os.listdir(dir):
    bd = os.path.join(dir, d)
    if os.path.isdir(bd): 
      try:
        shutil.rmtree(bd)
      except:
        pass

# функция, которая определяет разделитель в csv
def find_delimiter(file):
  sniffer = csv.Sniffer()
  with open(file) as f:
    delimiter = sniffer.sniff(f.readline()).delimiter
  return delimiter

# функция, которая определяет кодировку csv-файла
def get_encoding(file):
  with open(file, "rb") as f:
    tmp = chardet.detect(f.read(1000000))
  return tmp

# функция, которая  заполняет тип данных str
#                                                   предусмотреть сsv и xlsx !!!
def fill_dtype_str(file):
  res_dict = {}
  df = pd.read_csv(filepath_or_buffer = file, 
                    sep = find_delimiter(file = file), encoding = get_encoding(file = file)["encoding"]
                    , nrows = 5)
  cols = list(df.columns)
  for i in range(len(cols)):
    res_dict[cols[i]] = str
  return res_dict

# функция, которая сохраняет файлы из самой поздней папки в БД
def save_latest_file_to_db(schema):

  dir = 'all_data_for_save'

  if len(os.listdir(dir)) != 0:

    latest_folder = latest_subdir_of(p = dir)

    filenames = next(walk(latest_folder), (None, None, []))[2]  # [] if no file

    for i in range(len(filenames)):
        if filenames[i][-4:] == "xlsx":
            name_of_file_to_db = str(
                filenames[i][:-5].translate({ord(c): None for c in string.whitespace})
                + '_' + filenames[0][-4:-2])

            file_direct = latest_folder + "\\" + filenames[i]
            
            if os.path.getsize(file_direct) > 10**9:
                df = dask.read_excel(file_direct).convert_dtypes()

            elif os.path.getsize(file_direct) <= 10**9:
                df = pd.read_excel(file_direct).convert_dtypes()

            save_table_to_db(schema = schema, 
                    table_name = name_of_file_to_db, df = df, 
                    params = get_params_db())

        if filenames[i][-3:] == "csv":
            name_of_file_to_db = str(
                filenames[i][:-4].translate({ord(c): None for c in string.whitespace})
                + '_' + filenames[0][-3:])

            file_direct = latest_folder + "\\" + filenames[i]
            
            if os.path.getsize(file_direct) > 10**9:
                df = dask.read_csv(filepath_or_buffer = file_direct, 
                      sep = find_delimiter(file = file_direct)
                      , encoding = get_encoding(file = file_direct)["encoding"]).convert_dtypes() # потенциально может быть ошибка

            elif os.path.getsize(file_direct) <= 10**9:
                df = pd.read_csv(filepath_or_buffer = file_direct, 
                      sep = find_delimiter(file = file_direct)
                      , encoding = get_encoding(file = file_direct)["encoding"]).convert_dtypes()

            save_table_to_db(schema = schema, 
                    table_name = name_of_file_to_db, df = df, 
                    params = get_params_db())
            
        delete_all_in_dir(dir = dir)

# функция для получения списка опций для выпадающего списка
def get_list_options(schema = "public"):
    lst_of_tables = list(get_df_tables(schema = schema, params = get_params_db()).table_name.values)
    lst_dct = []
    for i in range(len(lst_of_tables)):
        dct = {}
        dct["label"] = str(lst_of_tables[i])
        dct["value"] = str(lst_of_tables[i])
        lst_dct.append(dct)
    return lst_dct
