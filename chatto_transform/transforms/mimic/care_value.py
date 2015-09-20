from chatto_transform.transforms.transform_base import Transform
from chatto_transform.schema.mimic import care_value_schema as cvs
from chatto_transform.schema.schema_base import MultiSchema, PartialSchema
from chatto_transform.schema.mimic.mimic_schema import \
    cptevents_schema, admissions_schema, patients_schema, ioevents_schema, icustayevents_schema
from chatto_transform.lib.chunks import left_join, horizontal_merge, from_chunks
from chatto_transform.sessions.mimic import mimic_common

import itertools
import pandas as pd
from math import ceil

class VasopressorDays(Transform):
    """Uses ioevents and icustayevents to get the number of days on vasopressors per icu stay.
    Calling load() will load for all relevant icustays in the database.
    You can also load those tables yourself and filter them down and pass directly into transform().
    """
    def __init__(self, carevue_only=False, metavision_only=False):
        if all([carevue_only, metavision_only]):
            raise TypeError("Can't use both carevue_only and metavision_only at once")
        self.carevue = not metavision_only
        self.metavision = not carevue_only

    def input_schema(self):
        return MultiSchema({
            'ioevents': PartialSchema.from_schema(ioevents_schema),
            'icustayevents': PartialSchema.from_schema(icustayevents_schema)
        })

    def _load(self):
        valid_vaso_ids = []
        if self.metavision:
            valid_vaso_ids.extend(VasoIOEventsMetavision.valid_vaso_ids)
        if self.carevue:
            valid_vaso_ids.extend(VasoIOEventsCarevue.valid_vaso_ids)
        ioevents_cond = "itemid in ( {} )".format(', '.join(map(str, sorted(valid_vaso_ids))))
        ioevents_cond += " and ((rate is not null and rate != 0) or (volume is not null and volume != 0))"
        ioevents = mimic_common.load_table(ioevents_schema, ioevents_cond)
        icustayevents = mimic_common.load_table(icustayevents_schema)
        return {'ioevents': ioevents, 'icustayevents': icustayevents}

    def _transform(self, tables):
        # filtering carevue ioevents
        io_carevue = VasoIOEventsCarevue().transform(tables)
        
        # filtering metavision ioevents
        io_metavision = VasoIOEventsMetavision().transform(tables)

        # concatenating ioevents
        ioevents = from_chunks([io_carevue, io_metavision])

        # getting vaso day counts
        vaso_days = VasoDayCounts().transform(ioevents)

        return vaso_days

class VasoIOEventsCarevue(Transform):
    vaso_mappings = {
        'levophed': [30047, 30120],
        'epinephrine': [30044, 30119, 30309],
        'neosynephrine': [30127, 30128],
        'vasopressin': [30051],
        'dopamine': [30043, 30307],
        'dobutamine': [30042, 30306],
        'milrinone': [30125]
    }
    valid_vaso_ids = list(itertools.chain.from_iterable(vaso_mappings.values()))
    vaso_types = list(vaso_mappings.keys())
    vaso_ids_ser = pd.Series(index=valid_vaso_ids)
    for vaso, vaso_ids in vaso_mappings.items():
        vaso_ids_ser.loc[vaso_ids] = vaso

    def input_schema(self):
        return MultiSchema({
            'ioevents': PartialSchema.from_schema(ioevents_schema),
            'icustayevents': PartialSchema.from_schema(icustayevents_schema)
        })

    def output_schema(self):
        return cvs.vaso_days_input

    def _load(self):
        ioevents_cond = "itemid in ( {} )".format(', '.join(map(str, sorted(self.valid_vaso_ids))))
        ioevents_cond += " and ((rate is not null and rate != 0) or (volume is not null and volume != 0))"
        ioevents = mimic_common.load_table(ioevents_schema, ioevents_cond)
        icustayevents = CarevueFilter().load()
        return {'ioevents': ioevents, 'icustayevents': icustayevents}

    def _transform(self, tables):
        icustayevents = CarevueFilter().transform(tables['icustayevents'])
        icustay_ids = icustayevents['icustay_id'].unique()

        ioevents = tables['ioevents']
        ioevents = ioevents[ioevents['icustay_id'].isin(icustay_ids)]
        ioevents = ioevents[ioevents['itemid'].isin(self.valid_vaso_ids)]
        ioevents = ioevents[(ioevents['rate'] != 0) | (ioevents['volume'] != 0)]

        ioevents['vaso_type'] = self.vaso_ids_ser.loc[ioevents['itemid']].values
        ioevents['vaso_type'] = ioevents['vaso_type'].astype('category')
        ioevents['date'] = ioevents['endtime'].dt.date
        ioevents = ioevents[cvs.vaso_days_input.col_names()]
        return ioevents

class CarevueFilter(Transform):
    def input_schema(self):
        return PartialSchema.from_schema(icustayevents_schema)

    def _load(self):
        return mimic_common.load_table(icustayevents_schema, "dbsource='mimic2v26'")

    def _transform(self, icustayevents):
        return icustayevents[icustayevents['dbsource'].isin(['mimic2v26'])]

class VasoIOEventsMetavision(Transform):
    vaso_mappings = {
        'levophed': [221906],
        'epinephrine': [221289],
        'neosynephrine': [221749],
        'vasopressin': [222315],
        'dopamine': [221662],
        'dobutamine': [221653],
        'milrinone': [221986]
    }
    valid_vaso_ids = list(itertools.chain.from_iterable(vaso_mappings.values()))
    vaso_types = list(vaso_mappings.keys())
    vaso_ids_ser = pd.Series(index=valid_vaso_ids)
    for vaso, vaso_ids in vaso_mappings.items():
        vaso_ids_ser.loc[vaso_ids] = vaso

    def input_schema(self):
        return MultiSchema({
            'ioevents': PartialSchema.from_schema(ioevents_schema),
            'icustayevents': PartialSchema.from_schema(icustayevents_schema)
        })

    def output_schema(self):
        return cvs.vaso_days_input

    def _load(self):
        ioevents_cond = "itemid in ( {} )".format(', '.join(map(str, sorted(self.valid_vaso_ids))))
        ioevents_cond += " and ((rate is not null and rate != 0) or (volume is not null and volume != 0))"
        ioevents = mimic_common.load_table(ioevents_schema, ioevents_cond)
        icustayevents = MetavisionFilter().load()
        return {'ioevents': ioevents, 'icustayevents': icustayevents}

    def _transform(self, tables):
        icustayevents = MetavisionFilter().transform(tables['icustayevents'])

        icustay_ids = icustayevents['icustay_id'].unique()

        ioevents = tables['ioevents']
        ioevents = ioevents[ioevents['icustay_id'].isin(icustay_ids)]
        ioevents = ioevents[ioevents['itemid'].isin(self.valid_vaso_ids)]
        ioevents = ioevents[(ioevents['rate'] != 0) | (ioevents['volume'] != 0)]

        ioevents['vaso_type'] = self.vaso_ids_ser.loc[ioevents['itemid']].values
        ioevents['vaso_type'] = ioevents['vaso_type'].astype('category')

        ioevents['end_date'] = pd.to_datetime(ioevents['endtime'].dt.date)
        ioevents['start_date'] = pd.to_datetime(ioevents['starttime'].dt.date)

        ioevents = ioevents[['subject_id', 'icustay_id', 'vaso_type', 'end_date', 'start_date']]
        ioevents = ExpandDateRange().transform(ioevents)
        ioevents = ioevents[cvs.vaso_days_input.col_names()]
        return ioevents

class MetavisionFilter(Transform):
    def input_schema(self):
        return PartialSchema.from_schema(icustayevents_schema)

    def _load(self):
        return mimic_common.load_table(icustayevents_schema, "dbsource='metavision'")

    def _transform(self, icustayevents):
        return icustayevents[icustayevents['dbsource'].isin(['metavision'])]

class ExpandDateRange(Transform):
    def input_schema(self):
        return cvs.date_range

    def output_schema(self):
        return cvs.date

    def _transform(self, df):
        df['days'] = df['end_date'] - df['start_date'] + pd.Timedelta(days=1)
        max_days = ceil(df['days'].max().days)
        df['date'] = df['start_date']

        expanded = [df]
        for i in range(2, max_days):
            prev = expanded[-1]
            rows_as_long_as = prev[prev['days'] >= pd.Timedelta(days=i)]
            rows_as_long_as.loc[:, 'date'] += pd.Timedelta(days=1)
            expanded.append(rows_as_long_as)

        df = from_chunks(expanded)
        return df

class VasoDayCounts(Transform):
    vaso_types = ['dobutamine', 'dopamine', 'epinephrine', 'levophed', 'milrinone', 'neosynephrine', 'vasopressin']

    def input_schema(self):
        return cvs.vaso_days_input

    def output_schema(self):
        return cvs.vaso_days_output

    def _transform(self, ioevents):
        grp = ioevents.groupby(['subject_id', 'icustay_id'])
        total_vaso_days = grp['date'].nunique()

        grp2 = ioevents.groupby(['subject_id', 'icustay_id', 'vaso_type'])
        
        vaso_days = grp2['date'].nunique().unstack('vaso_type')
        vaso_days['total_vasopressor_days'] = total_vaso_days

        #convert from int to timedelta
        vaso_days.loc[:, self.vaso_types + ['total_vasopressor_days']] *= pd.Timedelta(days=1)

        #rename columns to have _days at the end, for readability
        vaso_days.columns = [col + '_days' if col in self.vaso_types
                             else col
                             for col in vaso_days.columns]

        vaso_days.fillna(0, inplace=True)
        vaso_days = vaso_days.reset_index()
        return vaso_days



### Incomplete work on ventilator and days til death transforms

"""
so this query gives you the number of ventilator days for each hospital admission

with ventdays as
(
select SUBJECT_ID, HADM_ID
, count(chartdate) as NumVentDays
from mimic2v30.cptevents
where COSTCENTER = 'Resp'
and DESCRIPTION in 
(
'VENT MGMT, 1ST DAY (INVASIVE)'
, 'VENT MGMT;SUBSQ DAYS(INVASIVE)'
)
group by SUBJECT_ID, HADM_ID
)
select adm.SUBJECT_ID, adm.HADM_ID
, coalesce(ventdays.NumVentDays,0) as NumVentDays
from mimic2v30.admissions adm
left join ventdays
    on adm.hadm_id = ventdays.hadm_id
order by adm.subject_id, adm.hadm_id;"""

class VentDays(Transform):
    def __init__(self, subject_ids=None, hadm_ids=None):
        super().__init__()
        if subject_ids is not None:
            subject_ids = list(subject_ids)
        self.subject_ids = subject_ids

        if hadm_ids is not None:
            hadm_ids = list(hadm_ids)
        self.hadm_ids = hadm_ids

    cpt_cond = """
        COSTCENTER = 'Resp'
        and DESCRIPTION in 
        (
        'VENT MGMT, 1ST DAY (INVASIVE)'
        , 'VENT MGMT;SUBSQ DAYS(INVASIVE)'
        )
    """

    def input_schema(self):
        return MultiSchema({
            'cptevents': PartialSchema.from_schema(cptevents_schema),
            'admissions': PartialSchema.from_schema(admissions_schema)
        })

    def _load(self):
        conds = []
        if self.subject_ids is not None:
            conds.append('SUBJECT_ID IN ( {} )'.format(', '.join(map(str, self.subject_ids))))
        if self.hadm_ids is not None:
            conds.append('HADM_ID IN ( {} )'.format(', '.join(map(str, self.hadm_ids))))

        cpt_cond = ' and '.join(conds + [self.cpt_cond]) or None
        cptevents = mimic_common.load_table(cptevents_schema, self.cpt_cond)

        adm_cond = ' and '.join(conds) or None
        admissions = mimic_common.load_table(admissions_schema, adm_cond)

        return {'cptevents': cptevents, 'admissions': admissions}

    def _transform(self, tables):
        grp = tables['cptevents'].groupby(['subject_id', 'hadm_id'])
        ventdays = grp['chartdate'].nunique().reset_index()
        ventdays.columns = ['ventdays.subject_id', 'ventdays.hadm_id', 'ventdays']

        df = left_join(tables['admissions'], ventdays, left_on='hadm_id', right_on='ventdays.hadm_id')

        df['ventdays'].fillna(0, inplace=True)
        df.sort_index(by=['subject_id', 'hadm_id'], inplace=True)
        return df


class TimeUntilDeath(Transform):
    def __init__(self, subject_ids=None, hadm_ids=None):
        super().__init__()
        
        if subject_ids is not None:
            subject_ids = list(subject_ids)
        self.subject_ids = subject_ids

        if hadm_ids is not None:
            hadm_ids = list(hadm_ids)
        self.hadm_ids = hadm_ids

    def input_schema(self):
        return MultiSchema({
            'admissions': PartialSchema.from_schema(admissions_schema),
            'patients': PartialSchema.from_schema(patients_schema)    
        })

    def _load(self):
        adm_conds = []
        pat_cond = None
        if self.subject_ids is not None:
            pat_cond = 'SUBJECT_ID IN ( {} )'.format(', '.join(map(str, self.subject_ids)))
            adm_conds.append(subj_cond)
        if self.hadm_ids is not None:
            adm_conds.append('HADM_ID IN ( {} )'.format(', '.join(map(str, self.hadm_ids))))

        adm_cond = ' and '.join(adm_conds) or None
        admissions = mimic_common.load_table(admissions_schema, adm_cond)

        patients = mimic_common.load_table(patients_schema, pat_cond)

        return {'admissions': admissions, 'patients': patients}

    def _transform(self, tables):
        patients = tables['patients']
        patients_schema.add_prefix(patients)

        admissions = tables['admissions']
        admissions_schema.add_prefix(admissions)

        df = left_join(patients, admissions,
            left_on='patients.subject_id',
            right_on='admissions.subject_id')

        df['hosdead'] = df['admissions.deathtime'].notnull()
        df['time_of_death'] = df['patients.dod_hosp'].fillna(df['patients.dod_ssn'])
        df['time_until_death'] = df['time_of_death'] - df['admissions.dischtime']

        return df

