from chatto_transform.datastores.datastore_base import DataStore

class CachingDataStore(DataStore):
    def __init__(self, schema, load_ds, cache_ds):
        super().__init__(schema)
        self.load_ds = load_ds
        self.cache_ds = cache_ds

    def _load(self):
        if self.cache_ds.exists():
            return self.cache_ds.load()

        df = self.load_ds.load()
        
        try:
            self.cache_ds.store(df)
        except ValueError:
            return df

        return df
