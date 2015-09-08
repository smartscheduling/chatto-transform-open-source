import pandas
from chatto_transform.schema.schema_base import *
from chatto_transform.datastores.datastore_base import DataStore
from chatto_transform.lib.chunks import CHUNK_SIZE, from_chunks

import odo
import datashape

from tempfile import NamedTemporaryFile

import os
import stat

@cat.register_metadata('odo')
def _(self):
    return "'{}': string".format(self.name)

@id_.register_metadata('odo')
def _(self):
    return "'{}': option[int64]".format(self.name)

@dt.register_metadata('odo')
def _(self):
    return "'{}': datetime".format(self.name)

@delta.register_metadata('odo')
def _(self):
    return "'{}': units['microsecond']".format(self.name)

@big_dt.register_metadata('odo')
def _(self):
    return "'{}': datetime".format(self.name)

@num.register_metadata('odo')
def _(self):
    return "'{}': float64".format(self.name)

@bool_.register_metadata('odo')
def _(self):
    return "'{}': float64".format(self.name)

def schema_to_dshape(schema):
    dshape_str = 'var * {' + ', '.join(col.metadata('odo') for col in schema.cols) + '}'
    return datashape.dshape(dshape_str)

class OdoDataStore(DataStore):
    def __init__(self, schema, odo_target, storage_target_type='pandas'):
        super().__init__(schema)
        self.odo_target = odo_target
        self.storage_target_type = storage_target_type

    def storage_target(self):
        return self.storage_target_type

    def load(self):
        seq = odo.odo(self.odo_target, odo.chunks(pandas.DataFrame),
            chunksize=CHUNK_SIZE,
            dshape=schema_to_dshape(self.schema))
        
        def conv_chunks(chunks):
            for chunk in chunks:
                print('typechecking a chunk')
                self.schema.conform_df(chunk, skip_sort=True)
                yield chunk

        print('concatenating df chunks')
        df = from_chunks(conv_chunks(seq))
        
        return df

    def _store(self, df):
        odo.odo(df, self.odo_target) #, dshape=schema_to_dshape(self.schema))        


            