from chatto_transform.config import config
from chatto_transform.schema.ss.ss_sql_raw_schema import appointments
from chatto_transform.datastores.hdf_datastore import HdfDataStore
from chatto_transform.lib.chunks import from_chunks

import time

ds = HdfDataStore(appointments, config.data_dir+'test.hdf')

chunks = ds.load_chunks()

start = time.time()
df = from_chunks(chunks)
end = time.time()

print('took', end - start, 'seconds to load and concatenate all data')
