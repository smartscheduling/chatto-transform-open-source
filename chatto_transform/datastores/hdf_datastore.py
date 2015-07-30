from .datastore_base import DataStore
from ..schema.schema_base import *

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

class HdfDataStore(DataStore):
    def __init__(self, schema, hdf_file, fixed=True):
        self.hdf_file = hdf_file
        self.fixed = fixed
        super().__init__(schema)

    def storage_target(self):
        return 'hdf'

    def _chunk_filename(self, i):
        return self.hdf_file+'_chunk_' + str(i)

    def _load(self):
        df = pandas.read_hdf(self.hdf_file, self.schema.name)
        for col in self.schema.cols:
            if isinstance(col, big_dt):
                # converting big_dt column
                df[col.name] = df[col.name].map(datetime.datetime.fromtimestamp, na_action='ignore')
        if df is None:
            return None
        return df

    def _load_chunks(self):
        for i in count():
            f = self._chunk_filename(i)

            if not os.path.isfile(f):
                break

            chunk = pandas.read_hdf(f, self.schema.name)
            for col in self.schema.cols:
                if isinstance(col, big_dt):
                    # converting big_dt column
                    chunk[col.name] = chunk[col.name].map(datetime.datetime.fromtimestamp, na_action='ignore')
            yield chunk

    def _store(self, df):
        if len(df) == 0:
            raise ValueError("Cannot store an empty dataframe in HDF5 format.")
        hdf_format = 'fixed' if self.fixed else 'table'

        df.to_hdf(self.hdf_file, self.schema.name, format=hdf_format, mode='a', complevel=9, complib='blosc')

    def _store_chunks(self, chunks):
        hdf_format = 'fixed' if self.fixed else 'table'
        
        for i, chunk in enumerate(chunks):
            f = self._chunk_filename(i)
            chunk.to_hdf(
                f,
                self.schema.name,
                format=hdf_format,
                mode='a',
                complevel=9,
                complib='blosc')


    def _update(self, new_df):
        if not os.path.exists(self.hdf_file):
            self.store(new_df)
            return

        old_df = self.load()
        if old_df is None:
            # no existing table found, storing new data
            self._store(new_df)
            return 


        new_df = new_df.copy()
        
        # merging categories for categorical columns
        for col in self.schema.col_names():
            if hasattr(old_df[col], 'cat') and hasattr(new_df[col], 'cat'):
                all_categories = old_df[col].cat.categories.union(new_df[col].cat.categories)
                old_df[col] = old_df[col].cat.set_categories(all_categories)
                new_df[col] = new_df[col].cat.set_categories(all_categories)

        index = self.schema.options['index']

        old_df.set_index(index, drop=False, inplace=True)
        new_df.set_index(index, drop=False, inplace=True)
        
        #updating existing rows
        matches = old_df.index.intersection(new_df.index)
        new_rows = new_df.index.difference(old_df.index)

        if len(matches) > len(new_rows):
            old_rows = old_df.index.difference(new_df.index)
            #just append old_rows to new_df and you're done
            if len(old_rows):
                combined_df = new_df.append(old_df.loc[old_rows], ignore_index=True)
            else:
                combined_df = new_df
        else:
            old_df.loc[matches] = new_df.loc[matches]

            #adding new rows
            if len(new_rows):
                combined_df = old_df.append(new_df.loc[new_rows], ignore_index=True)
            else:
                combined_df = old_df

        combined_df.reset_index(drop=True, inplace=True)
        
        #storing updated rows
        self._store(combined_df)

    def exists(self):
        return os.path.isfile(self.hdf_file)

    def delete(self):
        with suppress(FileNotFoundError, IsADirectoryError):
                os.remove(self.hdf_file)

