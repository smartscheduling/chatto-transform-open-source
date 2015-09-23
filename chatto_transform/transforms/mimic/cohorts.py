from chatto_transform.transforms.transform_base import Transform
from chatto_transform.schema.schema_base import MultiSchema, PartialSchema
from chatto_transform.schema.mimic import mimic_schema
from chatto_transform.lib.chunks import left_join
from chatto_transform.sessions.mimic import mimic_common

from chatto_transform.transforms.mimic.care_value import DaysUntilDeath

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

    def _load(self):
        cond = 'itemid IN ( {} )'.format(', '.join(map(str, self.lab_item_ids)))
        labevents = mimic_common.load_table(mimic_schema.labevents_schema, cond)

        icustayevents = mimic_common.load_table(mimic_schema.icustayevents_schema)

        return {'labevents': labevents, 'icustayevents': icustayevents}

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

    def _load(self):
        cond = 'itemid IN ( {} )'.format(', '.join(map(str, self.ioevent_item_ids)))
        ioevents = mimic_common.load_table(mimic_schema.ioevents_schema, cond)

        icustayevents = mimic_common.load_table(mimic_schema.icustayevents_schema)

        return {'ioevents': ioevents, 'icustayevents': icustayevents}

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
    def __init__(self, icustay_death=True, hadm_death=True, death_within_12mo=True):
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
        if incr_data is not None and 'icustayevents' in incr_data:
            unique_ids = self.unique_ids_from_icustayevents(incr_data['icustayevents'])
        else:
            unique_ids = {}
        return DaysUntilDeath(**unique_ids).load(incr_data)

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



