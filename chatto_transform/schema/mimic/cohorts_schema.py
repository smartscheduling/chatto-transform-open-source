from chatto_transform.schema.schema_base import *

summary_fields = [
        'ICU stays',
        'Hospital admissions',
        'Patients',
        'Died during ICU stay',
        'Died during hospital admission',
        'Died within 12mo of hospital admission',
        'First careu nits',
        'Last care units',
        'Avg ICU length of stay',
        'Avg hospital admission length of stay'
    ]

cohort_summary_schema = Schema('cohort_summary', [
    num('icustays'),
    num('hadms'),
    num('patients'),
    num('icustay_deaths'),
    num('hadm_deaths'),
    num('12mo_deaths'),
    num('first_careunits'),
    num('last_careunits'),
    delta('avg_icu_los'),
    delta('avg_hadm_los')
])

icustay_detail_schema = Schema('icustay_detail', [
    delta('pre_icu_los'),
    dt('intime'),
    dt('outtime'),
    dt('deathtime'),
    delta('age'),
    bool_('elective_surgery'),
    bool_('emergency_surgery'),
    cat('age_group'),
    cat('diagnosis'),
    bool_('icustay_expire_flag'),
    bool_('hospital_expire_flag')
])