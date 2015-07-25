from ..transform_base import Transform

pipeline_type_error_msg = """Invalid transform list: Transform {i1}'s output schema does not match Transform {i2}'s input schema.
{i1} output schema: {i1s}
{i2} input schema: {i2s}"""

class PipelineTransformException(Exception):
    pass

class PipelineTransform(Transform):
    def __init__(self, transform_list, intermediate_storage=None):
        self.transform_list = list(transform_list)
        self.intermediate_storage = intermediate_storage
        self._check_transform_list()

    def _check_transform_list(self):
        if len(self.transform_list) == 0:
            raise TypeError('Invalid transform list: empty.')

        for i, ts in enumerate(zip(self.transform_list, self.transform_list[1:])):
            t1, t2 = ts
            if t1.output_schema() != t2.input_schema():
                raise TypeError(pipeline_type_error_msg.format(i1=i, i2=i+1, i1s=t1.output_schema(), i2s=t2.input_schema()))

    def input_schema(self):
        return self.transform_list[0].input_schema()

    def output_schema(self):
        return self.transform_list[-1].output_schema()

    def _load(self):
        return self.transform_list[0].load()

    def _transform(self, data):
        for i, t in enumerate(self.transform_list):
            try:
                data = t.transform(data)
            except Exception as exc:
                msg = 'Encountered exception while running transform #{}, {} in pipeline.'.format(i, t)
                raise PipelineTransformException(msg) from exc
        return data