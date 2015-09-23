import os.path
from contextlib import suppress
import os
import gc
import csv

import pandas as pd

from chatto_transform.datastores.datastore_base import DataStore
from chatto_transform.datastores import hdf_datastore #for storage target extensions
from chatto_transform.schema.schema_base import *
from chatto_transform.lib.chunks import CHUNK_SIZE


class AppendableHdfDataStore(DataStore):
    def __init__(self, schema, hdf_file, expected_rows=None):
        super().__init__(schema)
        self.hdf_file = hdf_file
        self.hdf_schema = hdf_rename_schema(self.schema)
        self.expected_rows = expected_rows

    def storage_target(self):
        return 'hdf_enc'

    def exists(self):
        return os.path.isfile(self.hdf_file)

    def delete(self):
        with suppress(FileNotFoundError, IsADirectoryError):
            os.remove(self.hdf_file)
        
        for col in self._categorical_cols():
            with suppress(FileNotFoundError, IsADirectoryError):
                os.remove(self._get_category_file(col))

    def _get_category_file(self, col):
        return self.hdf_file + '_' + col +'_categories.csv'

    def _any_categories(self, col):
        cat_file = self._get_category_file(col)
        return os.path.exists(cat_file)

    def _load_categories(self, col):
        cat_file = self._get_category_file(col)
        cat_df = pd.read_csv(cat_file, header=None, names=['categories'], dtype=str)
        if cat_df['categories'].dtype != 'object':
            cat_df['categories'] = cat_df['categories'].astype(str)
        return cat_df['categories']

    def _add_categories(self, col, categories, append=True):
        cat_file = self._get_category_file(col)
        cat_df = pd.DataFrame()
        cat_df['categories'] = categories
        with open(cat_file, 'a') as f:
            cat_df.to_csv(f, index=False, header=False)

    def _get_store(self):
        return pd.HDFStore(self.hdf_file, complevel=9, complib='blosc')

    def _get_data_columns(self):
        return self.schema.col_names()

    def _categorical_cols(self):
        for col in self.hdf_schema.cols:
            if not isinstance(col, cat):
                continue
            yield col.name

    def _category_index_to_series(self, index):
        ser = index.astype('object').to_series().reset_index(drop=True)
        return ser

    def _store(self, df):
        df = self._rename_df_to_hdf(df)
        col_categories = {}
        for col in self._categorical_cols():
            col_categories[col] = df[col].cat.categories
            df[col] = df[col].cat.codes.astype('int64')
        df.reset_index(drop=True, inplace=True)

        store = self._get_store()
        store.put(self.hdf_schema.name, df, format='table', dropna=False,
            data_columns=self._get_data_columns(), expectedrows=self.expected_rows,
            chunksize=CHUNK_SIZE)
        store.close()

        for col_name, categories in col_categories.items():
            if len(categories) > 0:
                categories = self._category_index_to_series(categories)
                self._add_categories(col_name, categories, append=False)
    
    def append(self, inc_df):
        inc_df = inc_df.copy()
        self.schema.conform_df(inc_df)
        inc_df.reset_index(drop=True, inplace=True)

        if not self.exists():
            return self._store(inc_df)

        inc_df = self._rename_df_to_hdf(inc_df)

        for col in self._categorical_cols():
            if not self._any_categories(col):
                if len(inc_df[col].cat.categories) > 0:
                    categories = self._category_index_to_series(inc_df[col].cat.categories)
                    self._add_categories(col, categories, append=False)
                inc_df[col] = inc_df[col].cat.codes.astype('int64')    

            else:
                old_categories = pd.Index(self._load_categories(col))
                new_categories = inc_df[col].cat.categories
                
                diff = new_categories.difference(old_categories)

                merged_categories = old_categories.append(diff)
                inc_df[col] = inc_df[col].cat.set_categories(merged_categories).cat.codes.astype('int64')
                if len(diff) > 0:
                    print('updating categories for', col)
                    categories = self._category_index_to_series(diff)
                    self._add_categories(col, categories)

        print('storing numeric data')
        store = self._get_store()
        nrows = store.get_storer(self.hdf_schema.name).nrows
        inc_df.index = pd.Series(inc_df.index) + nrows
        store.append(self.schema.name, inc_df, format='table', dropna=False,
            data_columns=self._get_data_columns(), expectedrows=self.expected_rows,
            chunksize=CHUNK_SIZE)
        store.close()

    def _load(self):
        store = self._get_store()
        df = store[self.schema.name]
        store.close()

        for col in self._categorical_cols():
            if self._any_categories(col):
                categories = self._load_categories(col)
            else:
                categories = []
            df[col] = pd.Categorical.from_codes(df[col], categories=categories, name=col)
        
        df = self._rename_df_from_hdf(df)
        return df

    def _rename_df_to_hdf(self, df):
        df = df[self.schema.col_names()]
        df.columns = self.hdf_schema.col_names()
        return df

    def _rename_df_from_hdf(self, df):
        df = df[self.hdf_schema.col_names()]
        df.columns = self.schema.col_names()
        return df

def hdf_name_to_pandas(col_name):
    return col_name.replace('__', '.')

def pandas_name_to_hdf(col_name):
    return col_name.replace('.', '__')

def hdf_rename_schema(schema):
    hdf_schema = Schema.rename(schema, pandas_name_to_hdf(schema.name),
        rename_cols={col: pandas_name_to_hdf(col) for col in schema.col_names()})
    if 'index' in hdf_schema.options:
        hdf_schema.options['index'] = pandas_name_to_hdf(hdf_schema.options['index'])
    if 'order_by' in hdf_schema.options:
        hdf_schema.options['order_by'] = [pandas_name_to_hdf(col) for col in hdf_schema.options['order_by']]
        
    return hdf_schema
