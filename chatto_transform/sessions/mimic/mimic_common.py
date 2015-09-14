from chatto_transform.sessions.mimic import mimic_login, mimic_widgets

from chatto_transform.datastores.sqlalchemy_datastore import SATableDataStore
from chatto_transform.datastores.hdf_datastore import HdfDataStore
from chatto_transform.datastores.csv_datastore import CsvDataStore
from chatto_transform.datastores.caching_datastore import CachingDataStore

from chatto_transform.schema.schema_base import PartialSchema

from sqlalchemy import create_engine
from sqlalchemy.sql import text

import pandas as pd

from IPython.display import FileLink, display
from ipywidgets import interact
import ipywidgets as widgets

import unicodedata
import re
import os.path


def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    """
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = unicode(re.sub('[^\w\s-]', '', value).strip().lower())
    return re.sub('[-\s]+', '-', value)

def _get_table_loader(schema, condition=None):
    if condition is not None:
        condition = [text(condition)]
    loader = SATableDataStore(schema, mimic_login.get_engine(), condition)
    return loader


### Data loading

def load_table(schema, condition=None):
    loader = _get_table_loader(schema, condition)
    
    local_storage_dir = mimic_login.get_local_storage_dir()
    if local_storage_dir:
        query_f_name = schema.name
        if condition is not None:
            query_f_name += '_' + condition
        query_f_name +='.hdf'
        query_f_name = os.path.join(local_storage_dir, query_f_name)
        cache = HdfDataStore(schema, query_f_name, fixed=True)
        loader = CachingDataStore(schema, loader, cache)
        
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

    display(ss, c, b)

    @b.on_click
    def on_button_clicked(b):
        display(store_csv(ss.value, c.value or None))

loaded_tables = {}

def query():
    ss = mimic_widgets.schema_select()
    c = mimic_widgets.where_clause_text()
    b = widgets.Button(description='Execute')

    display(ss, c, b)

    @b.on_click
    def on_button_clicked(b):
        result = load_table(ss.value, c.value or None)
        loaded_tables[ss.value.name] = result
        print('Loaded', ss.value.name, 'and stored in loaded_tables["{}"]'.format(ss.value.name))

