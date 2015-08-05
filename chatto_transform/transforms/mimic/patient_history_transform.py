from chatto_transform.transforms.transform_base import Transform

from chatto_transform.schema.schema_base import *
from chatto_transform.schema.mimic.mimic_schema import chartevents_schema, icustay_detail_schema, labevents_schema
from chatto_transform.schema.mimic.patient_history_schema import patient_history_schema

from chatto_transform.lib.big_dt_tools import big_dt_to_num, num_to_big_dt

import pandas as pd

import itertools

class ChartTransform(Transform):
    chart_mappings = {
        'HR': [211],
        'TEMPERATURE': [676, 677, 678, 679],
        'SYS ABP': [51],
        'NI SYS ABP': [455],
        'MAP': [52, 6702],
        'NI MAP': [456],
        'Arterial PH': [780],
        'PaO2': [779],
        'PaCO2': [778],
        'SaO2': [834, 3495],
        'GITUBE': [203, 3428],
        'WEIGHT': [581],
        'GCS': [198],
        'FIO2': [3420, 190],
        'VO2 SAT': [823],
        'MIXED VO2': [822],
        'PVO2': [859]
    }

    valid_chart_types = list(itertools.chain.from_iterable(chart_mappings.values()))
    chart_types_ser = pd.Series(index=valid_chart_types)
    for category, chart_types in chart_mappings.items():
        chart_types_ser.loc[chart_types] = category

    def input_schema(self):
        return PartialSchema.from_schema(chartevents_schema)

    def output_schema(self):
        return PartialSchema.from_schema(patient_history_schema)

    def _transform(self, chartevents):
        df = chartevents
        df = df[(df['itemid'].isin(ChartTransform.valid_chart_types.unique()))
                & (~df['value1num'].isnull())]

        df['category'] = ChartTransform.chart_types_ser.loc[df['itemid']]

        df['valuenum'] = df['value1num']

        temp_mask = df['itemid'].isin([678, 679])
        df.loc[temp_mask, 'valuenum'] = ((df.loc[temp_mask]['value1num'] - 32) * (5 / 9)).round(decimals=1)

        round_mask = df['itemid'].isin([676, 677, 581])
        df.loc[round_mask, 'valuenum'] = df.loc[round_mask]['value1num'].round(decimals=1)

        percent_mask = df['itemid'].isin([3420])
        df.loc[percent_mask, 'valuenum'] = df.loc[percent_mask]['value1num'] / 100

        return df

class DiasTransform(ChartTransform):
    def _transform(self, chartevents):
        df = chartevents
        df = df[(df['itemid'].isin([51, 455]))
                & (~df['value1num'].isnull())
                & (~df['value2num'].isnull())]

        dias_abp_mask = df['itemid'] == 51
        df.loc[dias_abp_mask, 'category'] = 'DIAS ABP'

        ni_dias_abp_mask = df['itemid'] == 455
        df.loc[ni_dias_abp_mask, 'category'] = 'NI DIAS ABP'

        df['valuenum'] = df['value2num']
        return df

class VentilateRespTransform(ChartTransform):
    itemids = [543, 544, 545, 619, 39, 535, 683, 720, 721, 722, 732]

    def _transform(self, chartevents):
        df = chartevents
        df = df[df['itemid'].isin(VentilateRespTransform.itemids)]

        df['category'] = 'MECH_VENT_FLAG'
        df['valuenum'] = 1
        df = df.drop_duplicates(subset=patient_history_schema.col_names())
        return df

class SpontaneousRespTransform(ChartTransform):
    def input_schema(self):
        return MultiSchema({
            'chartevents': chartevents_schema,
            'forced_vent': PartialSchema(cols=[id_('icustay_id')]) 
        })

    def _transform(self, dfs):
        forced_vent_icustay_ids = dfs['forced_vent']['icustay_id'].unique()
        df = dfs['chartevents']
        df = df[(df['itemid'].isin([615, 618]))
                & (~df['icustay_id'].isin(forced_vent_icustay_ids))
                & (~df['value1num'].isnull())]

        df['category'] = 'SPONTANEOUS_RESP'
        df['valuenum'] = df['value1num']


class LabTransform(Transform):
    lab_mappings = {
        'HCT': [50383],
        'WBC': [50316, 50468],
        'GLUCOSE': [50112],
        'BUN': [50177],
        'HCO3': [50172],
        'POTASSIUM': [50149],
        'SODIUM': [50159],
        'BILIRUBIN': [50170],
        'LACTACTE': [50010],
        'ALKALINE PHOSPHOTASE': [50061],
        'AST': [50073],
        'ALT': [50062],
        'CHOLESTEROL': [50085],
        'TROPONIN_T': [50189],
        'TROPONIN_I': [50188],
        'ALBUMIN': [50060],
        'MAGNESIUM': [50140],
        'PLATELETS': [50428],
        'CREATININE': [50090],
        'CHOLESTEROL': [50085]
    }

    valid_lab_types = list(itertools.chain.from_iterable(lab_mappings.values()))
    lab_types_ser = pd.Series(index=valid_lab_types)
    for category, lab_types in lab_mappings.items():
        lab_types_ser.loc[lab_types] = category

    def input_schema(self):
        return PartialSchema.from_schema(labevents_schema)

    def output_schema(self):
        return PartialSchema.from_schema(patient_history_schema)

    def _transform(self, labevents):
        df = labevents
        df = df[(df['itemid'].isin(LabTransform.valid_lab_types.unique()))
                & (~df['valuenum'].isnull())]

        df['category'] = LabTransform.lab_types_ser.loc[df['itemid']]
        return df

class DemographicTransform(Transform):
    def input_schema(self):
        return PartialSchema.from_schema(icustay_detail_schema)

    def output_schema(self):
        return PartialSchema.from_schema(patient_history_schema)

    def _transform(self, icustay_detail):
        df = icustay_detail

        computed_df = pd.DataFrame(index=df.index)
        computed_df['AGE'] = df['icustay_admit_age'].round()
        computed_df['GENDER'] = df['gender'].cat.set_categories(['F', 'M']).cat.codes
        computed_df['LOS'] = df['hospital_los'] / (60 * 24)
        computed_df['SURVIVAL'] = pd.to_timedelta(df['dod'] - df['icustay_intime']).dt.days
        computed_df['WEIGHT'] = df['weight_first']
        computed_df['HEIGHT'] = df['height'].round(decimals=1)
        computed_df['SAPS'] = df['sapsi_first']
        computed_df['SOFA'] = df['sofa_first']

        computed_df.set_index([df['subject_id'], big_dt_to_num(df['icustay_intime'])], inplace=True)
        stacked_ser = computed_df.stack(dropna=False)
        stacked_df = stacked_ser.reset_index()
        stacked_df.columns = ['subject_id', 'charttime', 'category', 'valuenum']

        stacked_df['charttime'] = num_to_big_dt(stacked_df['charttime'])

        return stacked_df



