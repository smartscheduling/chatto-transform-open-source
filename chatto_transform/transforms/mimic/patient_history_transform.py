from chatto_transform.transforms.transform_base import Transform
from chatto_transform.schema.schema_base import *

from chatto_transform.lib.mimic.session import load_table

from chatto_transform.schema.mimic.mimic_schema import \
    chartevents_schema, labevents_schema, ioevents_schema, icustayevents_schema
from chatto_transform.schema.mimic.patient_history_schema import \
    patient_history_schema, patient_history_relative_time_schema

from chatto_transform.lib.big_dt_tools import big_dt_to_num, num_to_big_dt
from chatto_transform.lib.chunks import from_chunks

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
        'PVO2': [859],
        'MECH_VENT_FLAG': [543, 544, 545, 619, 39, 535, 683, 720, 721, 722, 732],
        'SPONTANEOUS_RESP': [615, 618]
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
        df = df[(df['itemid'].isin(self.valid_chart_types))
                & (~df['value1num'].isnull())]

        df['category'] = self.chart_types_ser.loc[df['itemid']].values
        df['valuenum'] = df['value1num']

        temp_mask = df['itemid'].isin([678, 679])
        df.loc[temp_mask, 'valuenum'] = ((df.loc[temp_mask]['value1num'] - 32) * (5 / 9)).round(decimals=1)

        round_mask = df['itemid'].isin([676, 677, 581])
        df.loc[round_mask, 'valuenum'] = df.loc[round_mask]['value1num'].round(decimals=1)

        percent_mask = df['itemid'] == 3420
        df.loc[percent_mask, 'valuenum'] = df.loc[percent_mask]['value1num'] / 100

        ventilated_resp_mask = df['itemid'].isin(self.chart_mappings['MECH_VENT_FLAG'])
        df.loc[ventilated_resp_mask, 'valuenum'] = 1

        spontaneous_resp_mask = (df['itemid'].isin(self.chart_mappings['SPONTANEOUS_RESP'])) \
                                & (~df['icustay_id'].isin(df[ventilated_resp_mask]['icustay_id'].unique()))
        df.loc[spontaneous_resp_mask, 'valuenum'] = df.loc[spontaneous_resp_mask]['value1num']

        dias_df = df[(df['itemid'].isin([51, 455])) & (~df['value2num'].isnull())]
        dias_abp_mask = dias_df['itemid'] == 51
        dias_df.loc[dias_abp_mask, 'category'] = 'DIAS ABP'
        dias_df.loc[~dias_abp_mask, 'category'] = 'NI DIAS ABP'
        dias_df['valuenum'] = dias_df['value2num']

        df = df.append(dias_df, ignore_index=True)

        df = df[['subject_id', 'charttime', 'category', 'valuenum']]

        return df

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
        df = df[(df['itemid'].isin(LabTransform.valid_lab_types))
                & (~df['valuenum'].isnull())]

        df['category'] = self.lab_types_ser.loc[df['itemid']].values

        df = df[['subject_id', 'charttime', 'category', 'valuenum']]
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
        
        computed_df['LOS'] = df['los'] / (60 * 24)

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

class UrineTransform(Transform):
    urine_itemids = [651, 715, 55, 56, 57, 61, 65, 69, 85, 94, 96, 288, 405, 428, 473, 2042, 2068, 2111, 2119, 2130, 1922, 2810, 2859, 3053, 3462, 3519, 3175, 2366, 2463, 2507, 2510, 2592, 2676, 3966, 3987, 4132, 4253, 5927]

    def input_schema(self):
        return PartialSchema.from_schema(ioevents_schema)

    def output_schema(self):
        return PartialSchema.from_schema(patient_history_schema)

    def _transform(self, ioevents):
        df = ioevents
        df = df[(df['itemid'].isin(UrineTransform.urine_itemids)) & (~df['volume'].isnull())]

        df['category'] = 'URINE'
        df['valuenum'] = df['volume']

        df = df[['subject_id', 'charttime', 'category', 'valuenum']]

        return df

class PatientHistoryTransform(Transform):
    def __init__(self, subject_ids=None):
        if subject_ids is not None:
            subject_ids = list(subject_ids)
        self.subject_ids = subject_ids

    def _load(self):
        if self.subject_ids is not None:
            condition = 'subject_id IN ( {} )'.format(', '.join(map(str, self.subject_ids)))
        else:
            condition = None
        
        icustay_detail = load_table(icustay_detail_schema, condition + ' AND subject_icustay_seq = 1')
        chartevents = load_table(chartevents_schema, condition)
        labevents = load_table(labevents_schema, condition)
        ioevents = load_table(ioevents_schema, condition)
        
        return {
            'chartevents': chartevents,
            'labevents': labevents,
            'ioevents': ioevents,
            'icustay_detail': icustay_detail
        }

    def input_schema(self):
        return MultiSchema({
            'chartevents': chartevents_schema,
            'labevents': labevents_schema,
            'ioevents': ioevents_schema,
            'icustay_detail': icustay_detail_schema 
        })

    def output_schema(self):
        return patient_history_schema

    def _transform(self, dfs):
        icustay_index = dfs['icustay_detail'].set_index('icustay_id')

        for df_name in ['chartevents', 'labevents', 'ioevents']:
            df = dfs[df_name]
            df['icustay_intime'] = icustay_index.loc[df['icustay_id'], 'icustay_intime'].values
            df = df[df['charttime'] > df['icustay_intime']]
            dfs[df_name] = df

        demo_history = DemographicTransform().transform(dfs['icustay_detail'])
        chart_history = ChartTransform().transform(dfs['chartevents'])
        lab_history = LabTransform().transform(dfs['labevents'])
        urine_history = UrineTransform().transform(dfs['ioevents'])

        all_history = from_chunks([chart_history, lab_history, demo_history, urine_history])
        
        all_history['charttime'] = big_dt_to_num(all_history['charttime'])
        all_history = all_history.sort_index(by=['subject_id', 'charttime']).reset_index(drop=True)
        all_history['charttime'] = num_to_big_dt(all_history['charttime'])

        all_history.drop_duplicates(inplace=True)
        all_history = all_history[['subject_id', 'charttime', 'category', 'valuenum']]
        
        return all_history

class PatientHistoryRelativeTimeTransform(Transform):
    def input_schema(self):
        return patient_history_schema

    def output_schema(self):
        return patient_history_relative_time_schema

    def _transform(self, patient_history):
        for subject_id, subject_history in patient_history.groupby('subject_id'):
            start_time = subject_history['charttime'].iloc[0]
            patient_history.loc[subject_history.index, 'charttime'] = subject_history['charttime'] - start_time

        return patient_history