from .datastore_base import DataStore
from .csv_datastore import CSVDataStore

from boto.s3.key import Key

import io

class S3DataStore(DataStore):
    def __init__(self, schema, boto_bucket):
        self.boto_bucket = boto_bucket
        super.__init__(schema)

    def storage_target(self):
        return 'csv'

    def _store(self, df):
        k = Key(self.boto_bucket)
        k.key = self.schema.name

        with io.StringIO() as f:
            store = CSVDataStore(f, self.schema)
            store.store(df)

            f.seek(0)

            k.set_contents_from_file(f, encrypt_key=True)

    def _load(self):
        k = Key(self.boto_bucket)
        k.key = self.schema.name

        with io.StringIO() as f:
            k.get_contents_to_file(f)

            f.seek(0)

            store = CSVDataStore(f, self.schema)
            return store.load()
            
    def delete(self):
        self.boto_bucket.delete_key(self.schema.name)
