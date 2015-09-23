from chatto_transform.schema.schema_base import *
from chatto_transform.schema.mimic.mimic_schema import \
    ioevents_schema, icustayevents_schema

date_range = PartialSchema('date_range', [
    dt('start_date'),
    dt('end_date')
])

date = PartialSchema('date', [
    dt('date')
])

vaso_days_input = PartialSchema('vaso_days_input', [
    id_('subject_id'),
    id_('icustay_id'),
    cat('vaso_type'),
    dt('date')
])

vaso_days_output = Schema('vaso_days_output', [
    id_('subject_id'),
    id_('icustay_id'),
    delta('dobutamine_days'),
    delta('dopamine_days'),
    delta('epinephrine_days'),
    delta('levophed_days'),
    delta('milrinone_days'),
    delta('neosynephrine_days'),
    delta('vasopressin_days'),
    delta('total_vasopressor_days')
])

icu_hadm_map = MultiSchema({
    'icustayevents': PartialSchema('icustayevents', [
        id_('hadm_id'),
        id_('icustay_id'),
        id_('subject_id')
    ]),
    'admissions': PartialSchema('admissions', [
        id_('hadm_id'),
        id_('subject_id'),
    ])
})

days_until_death = PartialSchema('days_until_death', [
    id_('subject_id'),
    id_('hadm_id'),
    id_('icustay_id'),
    bool_('icustay_death'),
    bool_('hadm_death'),
    dt('time_of_death'),
    delta('time_until_death')
])