from chatto_transform.datastores.datastore_base import DataStore
from chatto_transform.schema.schema_base import *

import pandas
from pymongo import MongoClient
from bson.code import Code

from operator import itemgetter

def collection_unique_raw_attrs(db, collection_name):
    mapper = Code("""
        function () {
            for (var connection in this['raw']) { 
                for (var key in this['raw'][connection]) {
                    emit(connection+'.'+key, null);
                }
            }
        }
    """)
    reducer = Code("""
        function (key, stuff) {
            return null
        }
    """)
    res = db[collection_name].map_reduce(mapper, reducer, {'inline': True})
    return set(r['_id'] for r in res['results'])

def collection_unique_foriegn_keys(db, collection_name):
    mapper = Code("""
        function () {
            for (var foreign_key in this['foreign_keys']) {
                emit(foreign_key, null);
            }
        }
    """)
    reducer = Code("""
        function (key, stuff) {
            return null
        }
    """)
    res = db[collection_name].map_reduce(mapper, reducer, {'inline': True})
    return set(r['_id'] for r in res['results'])

base_col_list = [id_('_id'), id_('_update_id'), id_('external_id'), cat('record_type'), id_('syncable_ext_entity_id')]

def collection_to_schema(db, collection_name):
    raw_attrs = collection_unique_raw_attrs(db, collection_name)
    foreign_keys = collection_unique_foriegn_keys(db, collection_name)

    col_list = base_col_list[:]

    for raw_attr in raw_attrs:
        col_list.append(obj(raw_attr))

    for foreign_key in foreign_keys:
        col_list.append(obj('foreign_key.'+foreign_key))

    return Schema(collection_name, col_list)

class ChattoSyncDataStore(DataStore):
    def __init__(self, schema, db):
        super().__init__(schema)
        self._check_single_foreign_keys_option()
        self.db = db

    def _load(self):
        cursor = self.db[self.schema.name].find()
        collection = pandas.DataFrame.from_records(cursor)
        
        raw_df = pandas.DataFrame.from_records(collection['raw'])
        for connection in raw_df.columns:
            raw_attr_df = pandas.DataFrame.from_records(raw_df[connection])
            for raw_attr in raw_attr_df.columns:
                collection[connection+'.'+raw_attr] = raw_attr_df[raw_attr]

        foreign_key_df = pandas.DataFrame.from_records(collection['foreign_keys'])
        for f_key in foreign_key_df.columns:
            col = 'foreign_key.'+f_key
            collection[col] = foreign_key_df[f_key].apply(lambda xs: [str(x) for x in xs])
            
        del collection['raw'], collection['foreign_keys']

        for col in ['_id', '_update_id']:
            collection[col] = collection[col].apply(str)

        for f_key in self.schema.options.get('single_foreign_keys', []):
            col = 'foreign_key.'+f_key
            collection[col] = collection[col].apply(itemgetter(0))

        self.schema.conform_df(collection)
        
        return collection

    def _check_single_foreign_keys_option(self):
        for f_key in self.schema.options.get('single_foreign_keys', []):
            if not isinstance(f_key, str):
                raise TypeError('single_foreign_keys schema option must contain a list of column names. Got '+f_key)
            col = 'foreign_key.'+f_key
            for column in self.schema.cols:
                if column.name == col:
                    if not isinstance(column, id_):
                        raise TypeError('single_foreign_keys schema option must contain column names of id_ columns. {} is type {}'.format(
                            column.name, type(column)))
                    else:
                        break
            else:
                raise TypeError('Column name {} in single_foreign_keys schema option not present as a column in schema.'.format(col))
