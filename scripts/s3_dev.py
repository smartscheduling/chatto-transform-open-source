import boto

from chatto_transform.datastores.s3_datastore import S3DataStore
from chatto_transform.datastores.hdf_datastore import HdfDataStore

from chatto_transform.schema.ss.ss_sql_raw_schema import appointments

conn = boto.connect_s3()

bucket = conn.get_bucket('chatto')

print('loading data from hdf')
data = HdfDataStore(appointments, '/Users/dan/dev/data/test.hdf_chunk_0').load()

ds = S3DataStore(appointments, bucket)
ds.store(data)