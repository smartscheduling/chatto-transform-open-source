from chatto_transform.datastores.datastore_base import DataStore
from chatto_transform.schema.schema_base import *

import pandas
from pymongo import MongoClient
from bson.code import Code

def collection_attrs(db, collection_name):
    mapper = Code("""
        function () {
            for (var attr in this) { 
                emit(attr, null);
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

def collection_to_schema(db, collection_name):
    attrs = collection_attrs(db, collection_name)

    return Schema(collection_name, [obj(attr) for attr in attrs])

class MongoDBDataStore(DataStore):
    def __init__(self, schema, db):
        super().__init__(schema)
        self.db = db

    def _load(self):
        cursor = self.db[self.schema.name].find()
        collection = pandas.DataFrame.from_records(cursor)

        return collection