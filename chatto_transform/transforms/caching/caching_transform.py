from ..transform_base import Transform

class CachingTransform(Transform):
    def __init__(self, transform, chache_store, update=False):
        self.transform_obj = transform_obj
        self.cache_store = cache_store
        self.update = update

    def input_schema(self):
        return self.transform_obj.input_schema()

    def output_schema(self):
        return self.transform_obj.output_schema()

    def _load(self):
        return self.transform_obj._load()

    def _transform(self, data):
        return self.transform_obj._transform(data)

    def transform(self, data):
        if self.cache_store.exists():
            return self.cache_store._load()

        result = super().transform(data)

        if self.update:
            self.cache_store.update(result)
        else:
            self.cache_store.store(result)

        return result
