from chatto_transform.schema.schema_base import *

class Transform:
    """Base class for the Transform abstraction.
    A Transform captures the procedures for loading and transforming data from one form to another."""

    def load(self, incremental_data=None):
        args = []
        if incremental_data is not None:
            args.append(incremental_data)
        result = self._load(*args)
        self.input_schema().conform_df(result)
        return result

    def _load(self, incremental_data=None):
        raise NotImplementedError()

    def transform(self, data):
        data = data.copy()
        if isinstance(data, dict):
            for k, df in data.items():
                data[k] = df.copy()
        self.input_schema().conform_df(data)
        result = self._transform(data)
        del data
        self.output_schema().conform_df(result)
        return result

    def _transform(self, data):
        raise NotImplementedError()

    def input_schema(self):
        return PartialSchema()

    def output_schema(self):
        return PartialSchema()

    def load_transform(self, incremental_data=None):
        data = self.load(incremental_data)
        return self.transform(data)

class _InlineTransform(Transform):
    def __init__(self, input_schema, output_schema, load=None, transform=None):
        self._input_schema = input_schema
        self._output_schema = output_schema
        self._load_func = load
        self._transform_func = transform

    def input_schema(self):
        return self._input_schema

    def output_schema(self):
        return self._output_schema

    def _load(self):
        if self._load_func is None:
            raise NotImplementedError()
        return self._load_func()

    def _transform(self, data):
        if self._transform_func is None:
            raise NotImplementedError()
        return self._transform_func(data)

def make_transform(*, input_schema, output_schema, load=None, transform=None):
    return _InlineTransform(input_schema, output_schema, load, transform)