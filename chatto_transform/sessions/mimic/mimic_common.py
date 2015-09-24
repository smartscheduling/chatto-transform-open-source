from chatto_transform.sessions.mimic import mimic_login, mimic_widgets

from chatto_transform.datastores.sqlalchemy_datastore import SATableDataStore, SAQueryDataStore
from chatto_transform.datastores.appendable_datastore import AppendableHdfDataStore
from chatto_transform.datastores.hdf_datastore import HdfDataStore
from chatto_transform.datastores.csv_datastore import CsvDataStore
from chatto_transform.datastores.caching_datastore import CachingDataStore

from chatto_transform.schema.schema_base import PartialSchema

from sqlalchemy import create_engine
from sqlalchemy.sql import text
from psycopg2 import ProgrammingError

import pandas as pd
pd.options.display.max_columns = 10
pd.options.display.max_rows = 10

from IPython.display import FileLink, display
from ipywidgets import interact
import ipywidgets as widgets

import unicodedata
import re
import os.path
import shelve
import contextlib
import traceback
import sys

import warnings
warnings.filterwarnings('ignore')

class SQLError(Exception):
    pass

@contextlib.contextmanager
def sql_exception():
    try:
        yield
    except ProgrammingError as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(SQLError, exc_value, None)

def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    """
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = re.sub(b'[^\w\s-]', b'', value).strip().lower()
    return re.sub(b'[-\s]+', b'-', value).decode(encoding='UTF-8')

def _get_table_loader(schema, condition=None):
    if condition is not None:
        condition = [text(condition)]
    loader = SATableDataStore(schema, mimic_login.get_engine(), condition)
    return loader

def _get_sql_loader(sql):
    loader = SAQueryDataStore(PartialSchema('query'), mimic_login.get_engine(), text(sql))    
    return loader

### Data loading

def load_table(schema, condition=None):
    loader = _get_table_loader(schema, condition)
    
    local_storage_dir = mimic_login.get_local_storage_dir()
    if local_storage_dir:
        query_f_name = schema.name
        if condition is not None:
            db_file = mimic_login.get_db_file()
            with shelve.open(db_file) as db:
                qs_k = 'queries_'+schema.name
                if qs_k not in db:
                    db[qs_k] = ()
                condition = slugify(condition)
                if condition not in db[qs_k]:
                    db[qs_k] += (condition,)
                q_idx = db[qs_k].index(condition)
                query_f_name += '_query_' + str(q_idx)
        query_f_name +='.hdf'
        query_f_name = os.path.join(local_storage_dir, query_f_name)
        cache = AppendableHdfDataStore(schema, query_f_name)
        loader = CachingDataStore(schema, loader, cache)
    
    with sql_exception():
        return loader.load()

def load_sql(sql):
    loader = _get_sql_loader(sql)
    with sql_exception():
        return loader.load()

def store_csv(schema, condition=None):
    file_name = schema.name
    if not os.path.splitext(file_name)[1] == '.csv':
        file_name = file_name + '.csv'
    local_storage_dir = mimic_login.get_local_storage_dir()
    file_path = os.path.join(local_storage_dir, file_name)
    
    loader = _get_table_loader(schema, condition)
    loader.to_csv(file_path)
    return FileLink(os.path.relpath(file_path), result_html_prefix='Right-click and save: ')

def df_to_csv(file_name, df):
    if not os.path.splitext(file_name)[1] == '.csv':
        file_name = os.path.join(file_name, '.csv')
    sp = os.path.splitext(file_name)[0]
    local_storage_dir = mimic_login.get_local_storage_dir()
    file_path = os.path.join(local_storage_dir, file_name)
    
    store = CsvDataStore(PartialSchema(sp), file_path)
    store.store(df)
    return FileLink(os.path.relpath(file_path), result_html_prefix='Right-click and save: ')

def df_to_hdf5(file_name, df):
    if not os.path.splitext(file_name)[1] == '.hdf':
        file_name = os.path.join(file_name, '.hdf')
    sp = os.path.splitext(file_name)[0]
    local_storage_dir = mimic_login.get_local_storage_dir()
    file_path = os.path.join(local_storage_dir, file_name)

    store = HdfDataStore(PartialSchema(sp), file_path)
    store.store(df)
    return FileLink(os.path.relpath(file_path), result_html_prefix='Right-click and save: ')


### Widget common usage

def download_table():
    ss = mimic_widgets.schema_select()
    c = mimic_widgets.where_clause_text()
    b = widgets.Button(description='Execute')

    display(ss)
    display(c)
    display(b)

    @b.on_click
    def on_button_clicked(b):
        display(store_csv(ss.value, c.value or None))

loaded_tables = {}
def query():
    ss = mimic_widgets.schema_select()
    c = mimic_widgets.where_clause_text()
    b = widgets.Button(description='Execute')

    display(ss)
    display(c)
    display(b)    

    @b.on_click
    def on_button_clicked(b):
        result = load_table(ss.value, c.value or None)
        loaded_tables[ss.value.name] = result
        print('Loaded', ss.value.name, 'and stored in loaded_tables["{}"]'.format(ss.value.name))

loaded_sql = []
def sql():
    tb = mimic_widgets.query_text_box()
    b = widgets.Button(description='Execute')

    display(tb)
    display(b)

    @b.on_click
    def on_button_clicked(b):
        result = load_sql(tb.value)
        loaded_sql.append(result)
        print('Loaded', tb.value, 'and stored in loaded_sql[{}]'.format(len(loaded_sql) - 1))


