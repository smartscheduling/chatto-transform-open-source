from .datastore_base import DataStore
from .hdf_datastore import HdfDataStore

from ..lib import temp_file

from boto.s3.key import Key

import io

class S3DataStore(DataStore):
    def __init__(self, schema, boto_bucket):
        self.boto_bucket = boto_bucket
        super().__init__(schema)

    def storage_target(self):
        return 'hdf'

    def _store(self, df):
        tmp_path = temp_file.make_temporary_file()
        with temp_file.deleting(tmp_path):
            print('storing to temp hdf')
            store = HdfDataStore(self.schema, tmp_path)
            store._store(df)

            print('saving to s3')
            store_file_to_s3(self.boto_bucket, self.schema.name, tmp_path)

    def _store_chunks(self, chunks):
        pass

    def _load(self):
        tmp_path = temp_file.make_temporary_file()
        with temp_file.deleting(tmp_path):
            print('loading from s3')
            load_file_from_s3(self.boto_bucket, self.schema.name, tmp_path)

            print('loading from hdf')
            store = HdfDataStore(self.schema, tmp_path)
            return store._load()
            
    def delete(self):
        self.boto_bucket.delete_key(self.schema.name)


def store_file_to_s3(bucket, key_name, file_path):
    k = Key(bucket)
    k.key = key_name
    k.set_contents_from_filename(file_path)

def load_file_from_s3(bucket, key_name, file_path):
    k = Key(bucket)
    k.key = key_name
    k.get_contents_to_filename(file_path)