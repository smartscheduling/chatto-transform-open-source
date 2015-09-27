from chatto_transform.datastores.appendable_datastore import AppendableHdfDataStore
from chatto_transform.schema.mimic.mimic_schema import chartevents_schema
from chatto_transform.config import mimic_config

from pandas.io.pytables import Term
import pandas as pd

import os.path

EXPECTED_ROWS = 20000000

class CharteventsDataStore(AppendableHdfDataStore):
    def __init__(self):
        super().__init__(chartevents_schema, mimic_config.local_storage_dir+'/chartevents_store.h5', expected_rows=EXPECTED_ROWS)

    def _get_data_columns(self):
        return chartevents_schema.col_names()

    def select(self, where):
        store = self._get_store()

        df = store.select('chartevents', where=where, autoclose=True)

        for col in self._categorical_cols():
            if self._any_categories(col):
                print('loading categories for', col)
                categories = self._load_categories(col)
            else:
                categories = []
            df[col] = pd.Categorical.from_codes(df[col], categories=categories, name=col)
        
        df = self._rename_df_from_hdf(df)
        return df