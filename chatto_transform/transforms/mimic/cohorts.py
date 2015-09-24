from chatto_transform.transforms.transform_base import Transform
from chatto_transform.schema.schema_base import MultiSchema, PartialSchema
from chatto_transform.schema.mimic import mimic_schema, cohorts_schema
from chatto_transform.lib.chunks import left_join
from chatto_transform.sessions.mimic import mimic_common

from chatto_transform.transforms.mimic.care_value import DaysUntilDeath, AdmICUStayMapper

import pandas as pd

class LabItemFilter(Transform):
    @classmethod
    def valid_labitems(cls):
        labitems = mimic_common.load_table(mimic_schema.d_labitems_schema)
        return labitems

    def __init__(self, lab_item_ids=None):
        if lab_item_ids is not None:
            lab_item_ids = list(lab_item_ids)
        self.lab_item_ids = lab_item_ids

    def _load(self, incr_data=None):
        if incr_data is None:
            incr_data = {}

        if 'labevents' not in incr_data:
            cond = 'itemid IN ( {} )'.format(', '.join(map(str, self.lab_item_ids)))
            incr_data['labevents'] = mimic_common.load_table(mimic_schema.labevents_schema, cond)

        if 'icustayevents' not in incr_data:
            incr_data['icustayevents'] = mimic_common.load_table(mimic_schema.icustayevents_schema)

        return incr_data

    def input_schema(self):
        return MultiSchema({
            'labevents': mimic_schema.labevents_schema,
            'icustayevents': mimic_schema.icustayevents_schema
        })

    def _transform(self, tables):
        labevents = tables['labevents']
        if self.lab_item_ids is not None:
            labevents = labevents[labevents['itemid'].isin(self.lab_item_ids)]
        icustayevents = tables['icustayevents']

        df = left_join(
            labevents[['hadm_id', 'charttime']],
            icustayevents[['hadm_id', 'icustay_id', 'intime', 'outtime']],
            'hadm_id')
        df = df[(df['charttime'] >= df['intime']) & (df['charttime'] <= df['outtime'])]
        icustay_ids = df['icustay_id'].unique()
        return icustayevents[icustayevents['icustay_id'].isin(icustay_ids)]

class IOItemsFilter(Transform):
    @classmethod
    def valid_meditems(cls):
        meditems = mimic_common.load_table(mimic_schema.d_items_schema, "linksto = 'ioevents'")
        return meditems

    def __init__(self, ioevent_item_ids=None):
        if ioevent_item_ids is not None:
            ioevent_item_ids = list(ioevent_item_ids)
        self.ioevent_item_ids = ioevent_item_ids

    def _load(self, incr_data=None):
        if incr_data is None:
            incr_data = {}

        if 'ioevents' not in incr_data:
            cond = 'itemid IN ( {} )'.format(', '.join(map(str, self.ioevent_item_ids)))
            incr_data['ioevents'] = mimic_common.load_table(mimic_schema.ioevents_schema, cond)

        if 'icustayevents' not in incr_data:
            incr_data['icustayevents'] = mimic_common.load_table(mimic_schema.icustayevents_schema)

        return incr_data

    def input_schema(self):
        return MultiSchema({
            'ioevents': mimic_schema.ioevents_schema,
            'icustayevents': mimic_schema.icustayevents_schema
        })

    def _transform(self, tables):
        ioevents = tables['ioevents']
        if self.ioevent_item_ids is not None:
            ioevents = ioevents[ioevents['itemid'].isin(self.ioevent_item_ids)]
        icustayevents = tables['icustayevents']

        icustay_ids = ioevents['icustay_id'].unique()

        return icustayevents[icustayevents['icustay_id'].isin(icustay_ids)]


class DeathFilter(Transform):
    def __init__(self, icustay_death=False, hadm_death=False, death_within_12mo=False):
        self.icustay_death = icustay_death
        self.hadm_death = hadm_death
        self.death_within_12mo = death_within_12mo

    def input_schema(self):
        return MultiSchema({
            'admissions': PartialSchema.from_schema(mimic_schema.admissions_schema),
            'patients': PartialSchema.from_schema(mimic_schema.patients_schema),
            'icustayevents': PartialSchema.from_schema(mimic_schema.icustayevents_schema)
        })

    def unique_ids_from_icustayevents(self, icustayevents):
        ids = {}
        ids['icustay_ids'] = icustayevents['icustay_id'].unique()
        ids['hadm_ids'] = icustayevents['hadm_id'].unique()
        ids['subject_ids'] = icustayevents['subject_id'].unique()
        return ids

    def _load(self, incr_data=None):
        return DaysUntilDeath().load(incr_data)

    def _transform(self, tables):
        icustayevents = tables['icustayevents']

        unique_ids = self.unique_ids_from_icustayevents(icustayevents)

        df = DaysUntilDeath(**unique_ids).transform(tables)

        if self.icustay_death:
            df = df[df['icustay_death'] == 1]
        if self.hadm_death:
            df = df[df['hadm_death'] == 1]
        if self.death_within_12mo:
            df = df[df['time_until_death'] <= pd.Timedelta(days=365)]
        remaining_icustay_ids = df['icustay_id'].unique()

        return icustayevents[icustayevents['icustay_id'].isin(remaining_icustay_ids)]


class CohortSummary(Transform):
    def input_schema(self):
        return MultiSchema({
            'admissions': PartialSchema.from_schema(mimic_schema.admissions_schema),
            'patients': PartialSchema.from_schema(mimic_schema.patients_schema),
            'icustayevents': PartialSchema.from_schema(mimic_schema.icustayevents_schema)
        })

    def output_schema(self):
        return cohorts_schema.cohort_summary_schema

    def _load(self, incr_data=None):
        return DaysUntilDeath().load(incr_data)

    def _transform(self, tables):
        summary_columns = cohorts_schema.cohort_summary_schema.col_names()
        s = pd.Series(index=summary_columns)

        icu = tables['icustayevents']
        s.loc['icustays'] = icu['icustay_id'].nunique()
        s.loc['hadms'] = icu['hadm_id'].nunique()
        s.loc['patients'] = icu['subject_id'].nunique()

        death_df = DaysUntilDeath().transform(tables)
        s.loc['icustay_deaths'] = death_df['icustay_death'].sum()
        s.loc['hadm_deaths'] = death_df['hadm_death'].sum()
        s.loc['12mo_deaths'] = (death_df['time_until_death'] <= pd.Timedelta(days=365)).sum()

        s.loc['first_careunits'] = icu['first_careunit'].nunique()
        s.loc['last_careunits'] = icu['last_careunit'].nunique()

        s.loc['avg_icu_los'] = (icu['outtime'] - icu['intime']).mean()
        
        hadm = AdmICUStayMapper().load_transform({
            'admissions': tables['admissions'],
            'icustayevents': icu
        })
        s.loc['avg_hadm_los'] = (hadm['dischtime'] - hadm['admittime']).mean()
        
        result = pd.DataFrame([s], index=['summary'])
        return result

