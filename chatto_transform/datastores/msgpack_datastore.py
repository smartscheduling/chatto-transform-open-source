from chatto_transform.datastores.datastore_base import DataStore
from chatto_transform.schema.schema_base import *

import datetime
import pandas

for col_type in [dt, delta, big_dt, num, bool_]:
    col_type._storage_target_registry['msgpack'] = col_type._storage_target_registry['pandas'].copy()    

@cat.register_check('msgpack')
def _(col):
    return col.dtype == 'object'

@cat.register_transform('msgpack')
def _(col):
    if 'nan' not in col.cat.categories:
        col = col.cat.add_categories(['nan'])
    return col.fillna('nan').apply(str)

@id_.register_check('msgpack')
def _(col):
    return col.dtype == 'float64'

@id_.register_transform('msgpack')
def _(col):
    return col.astype('float64')

@big_dt.register_check('msgpack')
def _(col):
    return col.dtype == 'float64'

@big_dt.register_transform('msgpack')
def _(col):
    return col.map(datetime.datetime.timestamp, na_action='ignore')

class MsgpackDataStore(DataStore):
    def __init__(self, schema, buf):
        self.buf = buf
        super().__init__(schema)

    def storage_target(self):
        return 'msgpack'

    def _load(self):
        df = pandas.read_msgpack(self.buf)
        for col in self.schema.cols:
            if isinstance(col, big_dt):
                # converting big_dt column
                df[col.name] = df[col.name].map(datetime.datetime.fromtimestamp, na_action='ignore')

        return df

    def _load_chunks(self):
        for chunk in pandas.read_msgpack(self.buf, iterator=True):
            for col in self.schema.cols:
                if isinstance(col, big_dt):
                    # converting big_dt column
                    chunk[col.name] = chunk[col.name].map(datetime.datetime.fromtimestamp, na_action='ignore')
            yield chunk

    def _store(self, df):
        df.to_msgpack(self.buf)

    def _store_chunks(self, chunks):
        for chunk in chunks:
            print('storing chunk to msgpack')
            chunk.to_msgpack(self.buf, append=True)
            print('finished storing chunk to msgpack')

    def exists(self):
        return self.buf

