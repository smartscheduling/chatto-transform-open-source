import pandas
from ..schema.schema_base import *
from .datastore_base import DataStore

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

    def _load(self):
        seq = odo.odo(self.odo_target, odo.chunks(pandas.DataFrame),
            chunksize=65536)
            #dshape=schema_to_dshape(self.schema))

        print('concatenating df chunks')
        df = pandas.concat(seq, ignore_index=True)
        print('typechecking and sorting')
        return df

    def _store(self, df):
        #self.schema.conform_df(df, storage_target=self.storage_target_type, skip_sort=True)
        odo.odo(df, self.odo_target) #, dshape=schema_to_dshape(self.schema))        


            