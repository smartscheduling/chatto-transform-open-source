import ipywidgets as widgets
from IPython.display import display

from chatto_transform.sessions.mimic import mimic_widgets, mimic_common
from chatto_transform.transforms.mimic import cohorts
from chatto_transform.schema.mimic import mimic_schema

import pandas as pd

#51300

class Cohort:
    def __init__(self):
        self.icustay_detail_filters = {
            'age': None,
            'elective_surgery': None,
            'emergency_surgery': None,
            'age_group': None
        }
        self.medications = None
        self.labevents = None
        self.death = None
        self.icd9_codes = None

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

    def summary(self):
        icustayevents = self.load()
        summary = cohorts.CohortSummary().load_transform({
            'icustayevents': icustayevents    
        })
        return summary.T

    def icustay_ids(self):
        icustayevents = self.load()
        df = pd.DataFrame()
        df['icustay_ids'] = icustayevents['icustay_id'].unique()
        return df

    def load(self):
        incr_result = None
        if self.medications is not None:
            icustayevents = self.medications.load_transform()
            incr_result = {'icustayevents': icustayevents}
        if self.labevents is not None:
            icustayevents = self.labevents.load_transform(incr_result)
            incr_result = {'icustayevents': icustayevents}
        if self.death is not None:
            icustayevents = self.death.load_transform(incr_result)
            incr_result = {'icustayevents': icustayevents}
        if self.icd9_codes is not None:
            icd9_codes = self.icd9_codes.load_transform()
            icustayevents = cohorts.Icd9Filter(icd9_codes['icd9_codes'].values).load_transform(incr_result)           
            incr_result = {'icustayevents': icustayevents}

        if incr_result is None:
            icustayevents = mimic_common.load_table(mimic_schema.icustayevents_schema)
            incr_result = {'icustayevents': icustayevents}

        return incr_result['icustayevents']

    def filter_medications(self):
        self.medications = None
        ms = mimic_widgets.meditems_multiselect()
        b = widgets.Button(description='Execute')
        result = widgets.HTML(value="")
        display(ms, b, result)

        @b.on_click
        def on_button_clicked(b):
            self.medications = cohorts.IOItemsFilter(ms.value)
            result.value = 'Selected {} medications'.format(len(ms.value))

            ms.disabled = True
            b.disabled = True

    def filter_labevents(self):
        self.labevents = None
        ls = mimic_widgets.labitems_multiselect()
        b = widgets.Button(description='Execute')
        result = widgets.HTML(value="")
        display(ls, b, result)

        @b.on_click
        def on_button_clicked(b):
            self.labevents = cohorts.LabItemFilter(ls.value)
            result.value = 'Selected {} medications'.format(len(ls.value))

            ls.disabled = True
            b.disabled = True

    def filter_death(self):
        self.death = None
        ds = mimic_widgets.death_multiselect()
        b = widgets.Button(description='Execute')
        result = widgets.HTML(value="")
        display(ds, b, result)

        @b.on_click
        def on_button_clicked(b):
            kwds = {ds.value: True}
            self.death = cohorts.DeathFilter(**kwds)
            result.value = 'Selected {} death filter'.format(ds.value)

            ds.disabled = True
            b.disabled = True

    def filter_age(self):
        self.icustay_detail_filters['age'] = None
        f = mimic_widgets.num_range_filter()
        b = widgets.Button(description='Execute')
        result = widgets.HTML(value="")
        display(f, b, result)

        @b.on_click
        def on_button_clicked(b):
            self.icustay_detail_filters['age'] = f
            result.value = 'Selected age range'
            f.disabled = True
            b.disabled = True

    def filter_age_group(self):
        self.icustay_detail_filters['age_group'] = None
        f = mimic_widgets.cat_select(['neonate', 'middle', 'adult'])
        b = widgets.Button(description='Execute')
        result = widgets.HTML(value="")
        display(f, b, result)

        @b.on_click
        def on_button_clicked(b):
            self.icustay_detail_filters['age_group'] = f
            result.value = 'Selected age group'
            f.disabled = True
            b.disabled = True

    def filter_icd9_codes(self):
        self.icd9_codes = None
        t = widgets.HTML('Enter ICD9 Code and (Use % for wildcards)')
        f = mimic_widgets.multi_text_match()
        b = widgets.Button(description='Execute')
        result = widgets.HTML(value="")
        display(t, f, b, result)

        @b.on_click
        def on_button_clicked(b):
            self.icd9_codes = cohorts.Icd9Matcher(f)
            result.value = 'Selected ICD9 codes'
            f.disabled = True
            b.disabled = True