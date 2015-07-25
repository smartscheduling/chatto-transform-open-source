from .datastore_base import DataStore
from ..schema.schema_base import *

import pandas

for col_type in [cat, id_, dt, delta, num, bool_]:
    col_type._storage_target_registry['csv'] = col_type._storage_target_registry['pandas'].copy()

@cat.register_check('csv')
def _(col):
    return col.dtype == 'object'

@cat.register_transform('csv')
def _(col):
    return col.astype('object')

@id_.register_check('csv')
def _(col):
    return col.dtype == 'float64'
    
@id_.register_transform('csv')
def _(col):
    col = col.astype('float64')
    return col

class CsvDataStore(DataStore):
    def __init__(self, schema, file):
        self.file = file
        super().__init__(schema)

    def storage_target(self):
        return 'csv'

    def _load(self):
        df = pandas.read_csv(self.file)
        for col in self.schema.cols:
            if isinstance(col, dt):
                print('converting datetime column', col.name)
                df[col.name] = pandas.to_datetime(df[col.name], format="%Y-%m-%d %H:%M:%S", coerce=True)
        return df

    def _store(self, df):
        df.to_csv(self.file, index=False, date_format="%Y-%m-%d %H:%M:%S")


