from ..transform_base import Transform

class CachingTransform(Transform):
    def __init__(self, transform, datastore_factory, update=False):
        self.transform_obj = transform_obj
        self.datastore_factory = datastore_factory
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
        result = super().transform(data)

        datastore = self.datastore_factory(self.output_schema())
        if self.update:
            datastore.update(result)
        else:
            datastore.store(result)

        return result
