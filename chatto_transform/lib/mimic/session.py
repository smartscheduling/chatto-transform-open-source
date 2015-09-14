try:
    from chatto_transform.config import mimic_config, config
except ImportError:
    print('config/mimic_config.py not found. You will be unable to log into the database until you create this file.')
    mimic_config = None
    config = None

from chatto_transform.datastores.sqlalchemy_datastore import SATableDataStore
from chatto_transform.datastores.hdf_datastore import HdfDataStore
from chatto_transform.datastores.csv_datastore import CsvDataStore
from chatto_transform.datastores.caching_datastore import CachingDataStore

from sqlalchemy import create_engine
from sqlalchemy.sql import text

import pandas as pd
from IPython.display import FileLink

import shelve
import unicodedata
import re
import os.path

class MimicLoginException(Exception):
    pass

def create_engine_config(username, password):
    return config.mimic_psql_config
    engine_config_template = 'postgresql://{username}:{password}@mimic.coh8b3jmul4y.us-west-2.rds.amazonaws.com:5432/mimic2v26'
    engine_config = engine_config_template.format(username=username, password=password)
    return engine_config

def login():
    if mimic_config is None:
        raise MimicLoginException('Could not log in; no config/mimic_config.py found.')
    username = mimic_config.username
    password = mimic_config.password
    engine_config = create_engine_config(username, password)
    with shelve.open(get_session_path()) as db:
        db['engine_config'] = engine_config
    return create_engine(engine_config)

def get_session_path():
    return os.path.join(chatto_transform.config.__path__._path[0], '_mimic_session')

def get_engine():
    return create_engine(config.mimic_psql_config)
    with shelve.open(get_session_path()) as db:
        if 'engine_config' not in db:
            raise MimicLoginException('Must be logged in to access the database. Use login().')
        engine_config = db['engine_config']
        return create_engine(engine_config)

def enable_local_storage(storage_dir):
    with shelve.open(get_session_path()) as db:
        db['local_storage_dir'] = storage_dir

def get_local_storage_dir():
    with shelve.open(get_session_path()) as db:
        return db.get('local_storage_dir', None)

def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    """
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = unicode(re.sub('[^\w\s-]', '', value).strip().lower())
    return re.sub('[-\s]+', '-', value)

def load_table(schema, condition=None):
    loader = _get_table_loader(schema, condition)
    local_storage_dir = get_local_storage_dir()
    if local_storage_dir:
        query_f_name = schema.name
        if condition is not None:
            query_f_name += '_' + condition
        query_f_name +='.hdf'
        query_f_name = os.path.join(local_storage_dir, query_f_name)
        cache = HdfDataStore(schema, query_f_name, fixed=True)
        
        ds = CachingDataStore(schema, loader, cache)
        return ds.load()
    else:
        return loader.load()
    
def _get_table_loader(schema, condition=None):
    if condition is not None:
        condition = [text(condition)]
    loader = SATableDataStore(schema, get_engine(), condition)
    return loader

def store_csv(file_path, schema, condition=None):
    loader = _get_table_loader(schema, condition)
    loader.to_csv(file_path)
    return FileLink(file_path, result_html_prefix='Right-click and save: ')

def df_to_csv(file_path, df, schema):
    store = CsvDataStore(schema, file_path)
    store.store(df)
    return FileLink(file_path, result_html_prefix='Right-click and save: ')

def df_to_hdf5(file_path, df, schema):
    store = HdfDataStore(schema, file_path)
    store.store(df)
    return FileLink(file_path, result_html_prefix='Right-click and save: ')

def load_csv(file_path, schema):
    store = CsvDataStore(schema, file_path)
    df = store.load()
    return df

def load_hdf(file_path, schema):
    store = HdfDataStore(schema, file_path)
    df = store.load()
    return df
    