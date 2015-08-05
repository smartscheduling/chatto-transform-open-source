from chatto_transform.schema.schema_base import *

patient_history_schema = Schema('patient_history', [
    id_('subject_id'),
    big_dt('charttime'),
    cat('category'),
    num('valuenum')
])