from chatto_transform.transforms.transform_base import Transform
from chatto_transform.schema.mimic import care_value_schema as cvs
from chatto_transform.schema.schema_base import MultiSchema, PartialSchema
from chatto_transform.schema.mimic.mimic_schema import \
    cptevents_schema, admissions_schema, patients_schema, ioevents_schema, icustayevents_schema
from chatto_transform.lib.chunks import left_join, horizontal_merge, from_chunks
from chatto_transform.sessions.mimic import mimic_common

import itertools
import pandas as pd
import numpy as np
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
            return PartialSchema.from_schema(ioevents_schema)

    def _load(self):
        valid_vaso_ids = []
        if self.metavision:
            valid_vaso_ids.extend(VasoIOEventsMetavision.valid_vaso_ids)
        if self.carevue:
            valid_vaso_ids.extend(VasoIOEventsCarevue.valid_vaso_ids)
        ioevents_cond = "itemid in ( {} )".format(', '.join(map(str, sorted(valid_vaso_ids))))
        ioevents_cond += " and ((rate is not null and rate != 0) or (volume is not null and volume != 0))"
        ioevents = mimic_common.load_table(ioevents_schema, ioevents_cond)
        return ioevents

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
    vaso_ids_ser = vaso_ids_ser.astype('category')

    def input_schema(self):
        return PartialSchema.from_schema(ioevents_schema)

    def output_schema(self):
        return cvs.vaso_days_input

    def _load(self):
        ioevents_cond = "itemid in ( {} )".format(', '.join(map(str, sorted(self.valid_vaso_ids))))
        ioevents_cond += " and ((rate is not null and rate != 0) or (volume is not null and volume != 0))"
        ioevents = mimic_common.load_table(ioevents_schema, ioevents_cond)
        return ioevents

    def _transform(self, ioevents):
        ioevents = ioevents[ioevents['itemid'].isin(self.valid_vaso_ids)]
        ioevents = ioevents[(ioevents['rate'] != 0) | (ioevents['volume'] != 0)]
        
        ioevents['vaso_type'] = self.vaso_ids_ser.cat.codes.loc[ioevents['itemid']].values
        ioevents['vaso_type'] = pd.Categorical.from_codes(
            codes=ioevents['vaso_type'],
            categories=self.vaso_ids_ser.cat.categories,
            name='vaso_type'
        )
        ioevents['date'] = pd.to_datetime(ioevents['endtime'].dt.date)
        ioevents = ioevents[cvs.vaso_days_input.col_names()]
        return ioevents

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
    vaso_ids_ser = vaso_ids_ser.astype('category')

    def input_schema(self):
        return PartialSchema.from_schema(ioevents_schema)

    def output_schema(self):
        return cvs.vaso_days_input

    def _load(self):
        ioevents_cond = "itemid in ( {} )".format(', '.join(map(str, sorted(self.valid_vaso_ids))))
        ioevents_cond += " and ((rate is not null and rate != 0) or (volume is not null and volume != 0))"
        ioevents = mimic_common.load_table(ioevents_schema, ioevents_cond)
        return ioevents

    def _transform(self, ioevents):
        ioevents = ioevents[ioevents['itemid'].isin(self.valid_vaso_ids)]
        ioevents = ioevents[(ioevents['rate'] != 0) | (ioevents['volume'] != 0)]

        ioevents['vaso_type'] = self.vaso_ids_ser.cat.codes.loc[ioevents['itemid']].values
        ioevents['vaso_type'] = pd.Categorical.from_codes(
            codes=ioevents['vaso_type'],
            categories=self.vaso_ids_ser.cat.categories,
            name='vaso_type'
        )

        ioevents['end_date'] = pd.to_datetime(ioevents['endtime'].dt.date)
        ioevents['start_date'] = pd.to_datetime(ioevents['starttime'].dt.date)
        
        ioevents = ioevents[['subject_id', 'icustay_id', 'vaso_type', 'end_date', 'start_date']]
        ioevents = ExpandDateRange().transform(ioevents)
        ioevents = ioevents[cvs.vaso_days_input.col_names()]
        return ioevents

class ExpandDateRange(Transform):
    def input_schema(self):
        return cvs.date_range

    def output_schema(self):
        return cvs.date

    def _transform(self, df):
        df['days'] = df['end_date'] - df['start_date'] + pd.Timedelta(days=1)
        
        invalid_range_mask = df['days'] < 1 #remove rows with invalid (negative) deltas
        df = df[~invalid_range_mask]
        
        max_days = ceil(df['days'].max().days)
        df['date'] = df['start_date']
        del df['start_date'], df['end_date']

        expanded = [df]
        expand_per_row = False
        for i in range(2, max_days):
            prev = expanded[-1]
            rows_as_long_as = prev[prev['days'] >= pd.Timedelta(days=i)]
            rows_as_long_as.loc[:, 'date'] += pd.Timedelta(days=1)

            # if there are only a relative few rows remaining with long date ranges,
            # it will be more efficient to expand them per-row rather than per-date-range
            if len(rows_as_long_as) < max_days / len(rows_as_long_as.columns)**2:
                expand_per_row = True
                break
            expanded.append(rows_as_long_as)

        #expand remaining rows per-row rather than per-date-range
        if expand_per_row:
            rala = rows_as_long_as
            # represent any categorical columns as ints for fast expansion
            rala_cat = rala.select_dtypes(include=['category'])
            rala_categories = {}
            for col in rala_cat.columns:
                rala_categories[col] = rala[col].cat.categories
                rala[col] = rala[col].cat.codes

            # for each row, generate expanded rows as remaining days in the date range
            for j, row in rala.iterrows():
                date = row['date']
                n_days = row['days'].days - i
                expanded_row = pd.DataFrame(index=np.arange(n_days + 1), columns=row.index)
                for col in expanded_row.columns:
                    expanded_row[col] = row[col]
                expanded_row['date'] = pd.date_range(date, date+pd.Timedelta(days=n_days))
                
                #convert numeric back to categorical
                for col, categories in rala_categories.items():
                    expanded_row[col] = pd.Categorical.from_codes(
                        codes=expanded_row[col],
                        categories=categories,
                        name=col
                    )
                expanded.append(expanded_row)

        # merge all extra rows
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

class VentilatorDays(Transform):
    def __init__(self, subject_ids=None, hadm_ids=None):
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
            'icustayevents': PartialSchema.from_schema(icustayevents_schema)
        })

    def _load(self):
        conds = []
        if self.subject_ids is not None:
            conds.append('SUBJECT_ID IN ( {} )'.format(', '.join(map(str, self.subject_ids))))
        if self.hadm_ids is not None:
            conds.append('HADM_ID IN ( {} )'.format(', '.join(map(str, self.hadm_ids))))

        cpt_cond = ' and '.join(conds + [self.cpt_cond]) or None
        cptevents = mimic_common.load_table(cptevents_schema, cpt_cond)

        icu_cond = ' and '.join(conds) or None
        icustayevents = mimic_common.load_table(icustayevents_schema, icu_cond)

        return {'cptevents': cptevents, 'icustayevents': icustayevents}

    def _transform(self, tables):
        cpt = tables['cptevents']
        cpt = cpt[cpt['costcenter'].isin(['Resp'])]
        cpt = cpt[cpt['description'].isin([
            'VENT MGMT, 1ST DAY (INVASIVE)',
            'VENT MGMT;SUBSQ DAYS(INVASIVE)'
        ])]
        cpt = cpt[['subject_id', 'hadm_id', 'chartdate']] #filter columns
        icu = tables['icustayevents']

        if self.subject_ids is not None:
            cpt = cpt[cpt['subject_id'].isin(self.subject_ids)]
            icu = icu[icu['subject_id'].isin(self.subject_ids)]
        if self.hadm_ids is not None:
            cpt = cpt[cpt['hadm_id'].isin(self.hadm_ids)]
            icu = icu[icu['hadm_id'].isin(self.hadm_ids)]

        #add icustay_id
        df = AdmICUStayMapper().transform({
            'icustayevents': icu,
            'admissions': cpt
        })

        df = df[pd.to_datetime(df['intime'].dt.date) <= df['chartdate']]
        df = df[pd.to_datetime(df['outtime'].dt.date) >= df['chartdate']]

        grp = df.groupby(['subject_id', 'hadm_id', 'icustay_id'])
        ventdays = grp['chartdate'].nunique().reset_index()
        ventdays['ventdays'] = ventdays['chartdate'] * pd.Timedelta(days=1)
        del ventdays['chartdate']

        return ventdays

class TimeUntilDeath(Transform):
    def __init__(self, subject_ids=None, hadm_ids=None):
        if subject_ids is not None:
            subject_ids = list(subject_ids)
        self.subject_ids = subject_ids

        if hadm_ids is not None:
            hadm_ids = list(hadm_ids)
        self.hadm_ids = hadm_ids

    def input_schema(self):
        return MultiSchema({
            'admissions': PartialSchema.from_schema(admissions_schema),
            'patients': PartialSchema.from_schema(patients_schema),
            'icustayevents': PartialSchema.from_schema(icustayevents_schema)
        })

    def _load(self):
        adm_conds = []
        pat_cond = None
        if self.subject_ids is not None:
            pat_cond = 'SUBJECT_ID IN ( {} )'.format(', '.join(map(str, self.subject_ids)))
            adm_conds.append(pat_cond)
        if self.hadm_ids is not None:
            adm_conds.append('HADM_ID IN ( {} )'.format(', '.join(map(str, self.hadm_ids))))
        adm_cond = ' and '.join(adm_conds) or None

        admissions = mimic_common.load_table(admissions_schema, adm_cond)
        icustayevents = mimic_common.load_table(icustayevents_schema, adm_cond)
        patients = mimic_common.load_table(patients_schema, pat_cond)

        return {'admissions': admissions, 'icustayevents': icustayevents, 'patients': patients}

    def _transform(self, tables):
        pat = tables['patients']
        adm = tables['admissions']
        icu = tables['icustayevents']

        if self.subject_ids is not None:
            pat = pat[pat['subject_id'].isin(self.subject_ids)]
            adm = adm[adm['subject_id'].isin(self.subject_ids)]
            icu = icu[icu['subject_id'].isin(self.subject_ids)]
        if self.hadm_ids is not None:
            adm = adm[adm['hadm_id'].isin(self.hadm_ids)]
            icu = icu[icu['hadm_id'].isin(self.hadm_ids)]

        df = left_join(pat, adm, 'subject_id')

        df['hosdead'] = df['deathtime'].notnull()
        df['time_of_death'] = df['dod_hosp'].fillna(df['dod_ssn'])
        df['time_until_death'] = df['time_of_death'] - df['dischtime']

        df = AdmICUStayMapper().transform({
            'icustayevents': icu,
            'admissions': df
        })

        return df[['subject_id', 'hadm_id', 'icustay_id', 'hosdead', 'time_of_death', 'time_until_death']]

class AdmICUStayMapper(Transform):
    def input_schema(self):
        return cvs.icu_hadm_map

    def _transform(self, tables):
        ie = tables['icustayevents']
        adm = tables['admissions']

        df = left_join(ie, adm, ['hadm_id', 'subject_id'])
        return df

