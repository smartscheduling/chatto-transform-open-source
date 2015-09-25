from chatto_transform.transforms.transform_base import Transform
from chatto_transform.schema.schema_base import MultiSchema, PartialSchema
from chatto_transform.schema.mimic import mimic_schema, cohorts_schema
from chatto_transform.lib.chunks import left_join
from chatto_transform.sessions.mimic import mimic_common

from chatto_transform.transforms.mimic.care_value import DaysUntilDeath, AdmICUStayMapper

import pandas as pd
import numpy as np

import re

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

class Icd9Matcher(Transform):
    def __init__(self, text_filter):
        self.text_filter = text_filter

    def input_schema(self):
        ms = mimic_schema
        return MultiSchema({
            'd_icd_diagnoses': ms.d_icd_diagnoses_schema,
            'd_icd_procedures': ms.d_icd_procedures_schema
        })

    def _load(self, incr_data=None):
        if incr_data is None:
            incr_data = {}

        lt = mimic_common.load_table
        ms = mimic_schema
        if 'd_icd_diagnoses' not in incr_data:
            incr_data['d_icd_diagnoses'] = lt(ms.d_icd_diagnoses_schema)

        if 'd_icd_procedures' not in incr_data:
            incr_data['d_icd_procedures'] = lt(ms.d_icd_procedures_schema)

        return incr_data

    def _transform(self, tables):
        d_icd_diagnoses = tables['d_icd_diagnoses']
        d_icd_procedures = tables['d_icd_procedures']

        filters = {
            'icd9_code': self.text_filter
        }
        widget_filter = WidgetFilter(filters)

        d_icd_diagnoses = widget_filter.transform(d_icd_diagnoses)
        d_icd_procedures = widget_filter.transform(d_icd_procedures)

        unique_codes = pd.Index(d_icd_diagnoses['icd9_code'].unique())
        unique_codes = unique_codes.union(d_icd_procedures['icd9_code'].unique())
        unique_codes = pd.Series(unique_codes)
        df = pd.DataFrame()
        df['icd9_codes'] = unique_codes
        return df

class Icd9Filter(Transform):
    def __init__(self, icd9_codes=None):
        if icd9_codes is not None:
            self.icd9_codes = list(icd9_codes)

    def input_schema(self):
        ms = mimic_schema
        return MultiSchema({
            'diagnoses_icd': ms.diagnoses_icd_schema,
            'procedures_icd': ms.procedures_icd_schema,
            'icustayevents': ms.icustayevents_schema
        })

    def _load(self, incr_data=None):
        if incr_data is None:
            incr_data = {}

        if 'diagnoses_icd' not in incr_data:
            cond = 'icd9_code IN ( {} )'.format(', '.join(map(repr, self.icd9_codes)))
            incr_data['diagnoses_icd'] = mimic_common.load_table(mimic_schema.diagnoses_icd_schema, cond)

        if 'procedures_icd' not in incr_data:
            cond = 'icd9_code IN ( {} )'.format(', '.join(map(repr, self.icd9_codes)))
            incr_data['procedures_icd'] = mimic_common.load_table(mimic_schema.procedures_icd_schema, cond)

        if 'icustayevents' not in incr_data:
            incr_data['icustayevents'] = mimic_common.load_table(mimic_schema.icustayevents_schema)

        return incr_data

    def _transform(self, tables):
        diagnoses_icd = tables['diagnoses_icd']
        if self.icd9_codes is not None:
            diagnoses_icd = diagnoses_icd[diagnoses_icd['icd9_code'].isin(self.icd9_codes)]

        procedures_icd = tables['procedures_icd']
        if self.icd9_codes is not None:
            procedures_icd = procedures_icd[procedures_icd['icd9_code'].isin(self.icd9_codes)]
                
        icustayevents = tables['icustayevents']

        hadm_ids = pd.Index(diagnoses_icd['hadm_id'].unique())
        hadm_ids = hadm_ids.union(procedures_icd['hadm_id'].unique())

        return icustayevents[icustayevents['hadm_id'].isin(hadm_ids)]


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

class IcuStayDetail(Transform):
    def input_schema(self):
        ms = mimic_schema
        ps = PartialSchema.from_schema
        return MultiSchema({
            'icustayevents': ps(ms.icustayevents_schema),
            'admissions': ps(ms.admissions_schema),
            'patients': ps(ms.patients_schema),
            'services': ps(ms.services_schema)
        })

    def output_schema(self):
        return cohorts_schema.icustay_detail_schema


    def _load(self, incr_data=None):
        if incr_data is None:
            incr_data = {}

        lt = mimic_common.load_table
        ms = mimic_schema
        if 'icustayevents' not in incr_data:
            incr_data['icustayevents'] = lt(ms.icustayevents_schema)
        if 'admissions' not in incr_data:
            incr_data['admissions'] = lt(ms.admissions_schema)
        if 'patients' not in incr_data:
            incr_data['patients'] = lt(ms.patients_schema)
        if 'services' not in incr_data:
            incr_data['services'] = lt(ms.services_schema)

        return incr_data

    def _transform(self, tables):
        #group services by hadm_id and compute whether first service per hadm_id is a surgery
        services = tables['services']
        curr_service_cats = pd.Series(services['curr_service'].cat.categories)
        surgery_cats = curr_service_cats[curr_service_cats.str.contains('SURG')]
        surgery_codes = surgery_cats.index
        services['curr_service_code'] = services['curr_service'].cat.codes
        services.sort_index(by=['hadm_id', 'transfertime'], inplace=True)
        first_services = services.groupby('hadm_id', sort=False)['curr_service_code'].first().reset_index()
        first_services['is_surgery'] = first_services['curr_service_code'].isin(surgery_codes)

        #get the other tables
        icu = tables['icustayevents']
        adm = tables['admissions']    
        pat = tables['patients']
    
        #assign_prefixes to all of them before the join
        df_prefix = [
            (first_services, 's'),
            (icu, 'icu'),
            (adm, 'adm'),
            (pat, 'pat')
        ]
        for df, prefix in df_prefix:
            df.columns = [prefix + '.' + col for col in df.columns]

        j_df = left_join(icu, adm, left_on='icu.hadm_id', right_on='adm.hadm_id')
        j_df = left_join(j_df, pat, left_on='icu.subject_id', right_on='pat.subject_id')
        j_df = left_join(j_df, first_services, left_on='icu.hadm_id', right_on='s.hadm_id')

        df = pd.DataFrame(index=j_df.index)
        df['icustay_id'] = j_df['icu.icustay_id']
        df['pre_icu_los'] = j_df['icu.intime'] - j_df['adm.admittime']
        df['intime'] = j_df['icu.intime']
        df['outtime'] = j_df['icu.outtime']
        df['deathtime'] = j_df['adm.deathtime']
        df['age'] = j_df['icu.intime'] - j_df['pat.dob']
        df['elective_surgery'] = (j_df['adm.admission_type'].isin(['ELECTIVE'])) & j_df['s.is_surgery']
        df['emergency_surgery'] = (j_df['adm.admission_type'].isin(['EMERGENCY', 'URGENT'])) & j_df['s.is_surgery']
        
        df['age_group'] = 1
        df.loc[df['age'] / np.timedelta64(1, 'M') <= 1, 'age_group'] = 0
        df.loc[df['age'] / np.timedelta64(1, 'M') > 16, 'age_group'] = 2
        df['age_group'] = pd.Categorical.from_codes(df['age_group'], categories=['neonate', 'middle', 'adult'])
        
        df['diagnosis'] = j_df['adm.diagnosis']
        
        df['icustay_expire_flag'] = j_df['adm.deathtime'] <= j_df['icu.outtime'] # covers when deathtime is before, due to typographical errors
        df['icustay_expire_flag'] |= (j_df['adm.dischtime'] <= j_df['icu.outtime']) & j_df['adm.discharge_location'].isin(['DEAD/EXPIRED'])

        df['hospital_expire_flag'] = j_df['adm.discharge_location'].isin(['DEAD/EXPIRED'])

        return df
 

class IcustayDetailFilter(Transform):
    options = {
        'age': 'num',
        'elective_surgery': 'bool',
        'emergency_surgery': 'bool',
        'age_group': 'cat'
    }

    def input_schema(self):
        return MultiSchema({
            'icustayevents': ps(ms.icustayevents_schema),
            'icustay_detail': cohorts_schema.icustay_detail_schema
        })

    def _transform(self, tables):
        icu = tables['icustayevents']
        icustay_ids = icu['icustay_id'].unique()

        detail = tables['icustay_detail']
        detail = detail[detail['icustay_id'].isin(icustay_ids)]


class WidgetFilter(Transform):
    def __init__(self, filters):
        self.filters = dict(**filters)

    def _transform(self, df):
        for col, f in self.filters.items():
            if f.filter_type == 'text':
                cats = pd.Series(df[col].cat.categories)
                codes = df[col].cat.codes
                pats = []
                for pat in f.value:
                    pat = pat.replace('.', '\.')
                    pat = pat.replace('%', '.*')
                    pat = '^'+pat+'$'
                    pats.append(pat)
                cats = cats[cats.str.match('|'.join(pats))]
                f_codes = cats.index
                df = df[codes.isin(f_codes)]
            if f.filter_type == 'cat':
                df = df[df[col].isin(f.value)]
            if f.filter_type == 'num_eq':
                df = df[df[col] == f.value]
            if f.filter_type == 'num_range':
                if f.min_enabled:
                    df = df[df[col] >= f.min_value]
                if f.max_enabled:
                    df = df[df[col] <= f.max_value]
        return df
