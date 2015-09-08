from chatto_transform.datastores.datastore_base import DataStore
from chatto_transform.schema.schema_base import *

import datetime
from contextlib import suppress
from itertools import count
import pandas
import os
import os.path

import time

for col_type in [dt, delta, big_dt, num, bool_]:
    col_type._storage_target_registry['hdf'] = col_type._storage_target_registry['pandas'].copy()    

@cat.register_check('hdf')
def _(col):
    return col.dtype == 'object'

@cat.register_transform('hdf')
def _(col):
    if 'nan' not in col.cat.categories:
        col = col.cat.add_categories(['nan'])
    return col.fillna('nan').apply(str)

@id_.register_check('hdf')
def _(col):
    return col.dtype == 'float64'

@id_.register_transform('hdf')
def _(col):
    return col.astype('float64')

@big_dt.register_check('hdf')
def _(col):
    return col.dtype == 'float64'

@big_dt.register_transform('hdf')
def _(col):
    return col.map(datetime.datetime.timestamp, na_action='ignore')


for col_type in [id_, dt, delta, big_dt, num, bool_]:
    col_type._storage_target_registry['hdf_table'] = col_type._storage_target_registry['hdf'].copy()

@cat.register_check('hdf_table')
def _(col):
    return col.dtype == 'category'

@cat.register_transform('hdf_table')
def _(col):
    return col.astype('category')

class HdfDataStore(DataStore):
    def __init__(self, schema, hdf_file, fixed=True, encode_categoricals=True):
        self.hdf_file = hdf_file
        self.fixed = fixed
        self.encode_categoricals = encode_categoricals
        super().__init__(schema)

    def storage_target(self):
        return 'hdf' if self.fixed else 'hdf_table'

    def _chunk_filename(self, i):
        return self.hdf_file+'_chunk_' + str(i)

    def _read_hdf(self, f):
        store = pandas.HDFStore(f)
        df = store[self.schema.name]

        if self.encode_categoricals:
            col_categories = store.get_storer(self.schema.name).attrs.metadata['column_categories']
            for col, categories in col_categories.items():
                df[col] = pandas.Categorical.from_codes(df[col], categories, name=col)
        store.close()
        return df

    def _store_hdf(self, f, df):
        store = pandas.HDFStore(f, complevel=9, complib='blosc')

        col_categories = {}
        if self.encode_categoricals:
            for col in df.select_dtypes(include=['category']):
                col_categories[col] = df[col].cat.categories
                df[col] = df[col].cat.codes

        hdf_format = 'fixed' if self.fixed else 'table'
        store.put(self.schema.name, df, format=hdf_format, dropna=False)

        if self.encode_categoricals:
            store.get_storer(self.schema.name).attrs.metadata = {'column_categories': col_categories}

        store.close()

    def _load(self):
        df = self._read_hdf(self.hdf_file)
        for col in self.schema.cols:
            if isinstance(col, big_dt):
                # converting big_dt column
                df[col.name] = df[col.name].map(datetime.datetime.fromtimestamp, na_action='ignore')
        if df is None:
            return None
        return df

    def chunk_stores(self):
        for i in count():
            f = self._chunk_filename(i)

            if not os.path.isfile(f):
                break

            yield type(self)(self.schema, f, self.fixed, self.encode_categoricals)

    def _load_chunks(self):
        for store in self.chunk_stores():
            chunk = store._load()
            yield chunk

    def _store(self, df):
        if len(df) == 0:
            raise ValueError("Cannot store an empty dataframe in HDF5 format.")
        
        self._store_hdf(self.hdf_file, df)
       
    def _store_chunks(self, chunks):
        hdf_format = 'fixed' if self.fixed else 'table'
        
        for i, chunk in enumerate(chunks):
            f = self._chunk_filename(i)
            self._store_hdf(f, chunk)

    def exists(self):
        return os.path.isfile(self.hdf_file)

    def delete(self):
        with suppress(FileNotFoundError, IsADirectoryError):
            os.remove(self.hdf_file)

    def delete_chunks(self):
        for i in count():
            f = self._chunk_filename(i)

            if not os.path.isfile(f):
                break

            with suppress(FileNotFoundError, IsADirectoryError):
                os.remove(f)            

