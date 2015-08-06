from chatto_transform.schema.schema_base import *

patient_history_schema = Schema('patient_history', [
    id_('subject_id'),
    big_dt('charttime'),
    cat('category'),
    num('valuenum')
])

patient_history_relative_time_schema = Schema('patient_history_relative_time', [
    id_('subject_id'),
    delta('charttime'),
    cat('category'),
    num('valuenum')
])
