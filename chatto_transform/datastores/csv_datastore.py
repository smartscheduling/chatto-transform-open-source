from .datastore_base import DataStore
from ..schema.schema_base import *

import os.path
import pandas
import gzip
import io

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
    def __init__(self, schema, file, compress=False):
        self.file = file
        self.compress = compress
        super().__init__(schema)

    def storage_target(self):
        return 'csv'

    def _load(self):
        if self.compress:
            compression = 'gzip'
        else:
            compression = None
        df = pandas.read_csv(self.file, compression=compression)
        for col in self.schema.cols:
            if isinstance(col, dt):
                print('converting datetime column', col.name)
                df[col.name] = pandas.to_datetime(df[col.name], format="%Y-%m-%d %H:%M:%S", coerce=True)
        return df

    def _store(self, df):
        if self.compress:
            with io.StringIO() as f:
                df.to_csv(f, index=False, date_format="%Y-%m-%d %H:%M:%S")
                f.seek(0)
                compressed = gzip.compress(f.read().encode('utf8'))
                if isinstance(self.file, str):
                    with open(self.file, 'wb') as out:
                        out.write(compressed)
                else:
                    self.file.write(compressed)
        else:
            df.to_csv(self.file, index=False, date_format="%Y-%m-%d %H:%M:%S")

    def exists(self):
        return os.path.isfile(self.hdf_file)