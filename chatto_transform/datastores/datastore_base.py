class DataStore:
    """Base class - defines the DataStore abstraction.
    A DataStore is an adapter between a pandas DataFrame and a storage medium.
    Examples of storage media: HDF5 file format, SQL databases backed by SqlAlchemy,
     PostgreSQL databases."""
    def __init__(self, schema):
        self.schema = schema

    def storage_target(self):
        raise NotImplementedError()

    def load(self):
        result = self._load()
        self.schema.conform_df(result)
        return result

    def load_chunks(self):
        for chunk in self._load_chunks():
            self.schema.conform_df(chunk)
            yield chunk

    def store(self, df):
        df = df.copy()
        self.schema.conform_df(df, storage_target=self.storage_target())
        self._store(df)
        del df #delete our copy

    def store_chunks(self, chunks):
        def conform_each(chunks):
            for chunk in chunks:
                chunk = chunk.copy()
                self.schema.conform_df(chunk, storage_target=self.storage_target())
                yield chunk
                del chunk
        self._store_chunks(conform_each(chunks))

    def update(self, df):
        df = df.copy()
        self.schema.conform_df(df, storage_target=self.storage_target())
        self._update(df)
        del df #delete our copy

    def delete(self):
        raise NotImplementedError()

    def _load(self):
        raise NotImplementedError()

    def _load_chunks(self):
        raise NotImplementedError()
        yield

    def _store(self, df):
        raise NotImplementedError()

    def _store_chunks(self, chunk):
        raise NotImplementedError()

    def _update(self, df):
        raise NotImplementedError()
