from chatto_transform.datastores.datastore_base import DataStore
from chatto_transform.schema.schema_base import *

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


@cat.register_metadata('csv_dtype')
def _(self):
    return (self.name, 'object')

@id_.register_metadata('csv_dtype')
def _(self):
    return (self.name, 'float64')

@dt.register_metadata('csv_dtype')
@delta.register_metadata('csv')
def _(self):
    return (self.name, 'object')

@num.register_metadata('csv_dtype')
def _(self):
    return (self.name, 'float64')

@bool_.register_metadata('csv_dtype')
def _(self):
    return (self.name, 'float64')


class CsvDataStore(DataStore):
    def __init__(self, schema, file, compress=False, with_header=True, na_values=None):
        self.file = file
        self.compress = compress
        self.with_header = with_header
        self.na_values = na_values
        super().__init__(schema)

    def storage_target(self):
        return 'csv'

    def _load(self):
        print ('loading csv')
        if self.compress:
            compression = 'gzip'
        else:
            compression = None

        dtype_dict = dict(col.metadata('csv_dtype') for col in self.schema.cols)

        kwargs = {}
        if not self.with_header:
            kwargs['header'] = None
            kwargs['names'] = self.schema.col_names()
        if self.na_values is not None:
            kwargs['na_values'] = self.na_values

        df = pandas.read_csv(self.file, compression=compression, dtype=dtype_dict, **kwargs)
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
