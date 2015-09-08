from chatto_transform.transforms.transform_base import Transform

from chatto_transform.schema import schema_base as sb
from chatto_transform.schema.mimic.mimic_schema import admissions_schema, patients_schema

from chatto_transform.lib.mimic.session import load_table

import pandas as pd
import numpy as np

"""
Implements the age count query, given here:

select bucket+15, count(*) from (
    select
        months_between(ad.admit_dt, dp.dob)/12,
        width_bucket(months_between(ad.admit_dt, dp.dob)/12, 15, 100, 85) as bucket
    from
        mimic2v26.admissions ad,
        mimic2v26.d_patients dp
    where
        ad.subject_id = dp.subject_id
        and months_between(ad.admit_dt, dp.dob)/12 between 15 and 199
) sub
group by bucket
order by bucket;
"""

class AgeTransform(Transform):
    def input_schema(self):
        return sb.MultiSchema({
            'admissions': admissions_schema,
            'patients': patients_schema
        })

    def _load(self):
        """Load the two tables (admissions and d_patients) separately,
        since both are relatively small and can be efficiently joined in memory."""

        admissions = load_table(admissions_schema)
        patients = load_table(patients_schema)

        return {'admissions': admissions, 'patients': patients}

    def _transform(self, tables):
        """Join the two tables on subject_id and convert their age to years,
        cutting off at <15 and >100"""
        admissions = tables['admissions']
        patients = tables['patients']

        input_schema = self.input_schema()

        input_schema['admissions'].add_prefix(admissions)
        input_schema['patients'].add_prefix(patients)

        df = pd.merge(admissions, patients, how='left',
            left_on='admissions.subject_id', right_on='patients.subject_id')

        df['age_at_admission'] = df['admissions.admittime'] - df['patients.dob']
        df['age_at_admission'] = df['age_at_admission'] / np.timedelta64(1, 'Y') #convert to years
        df['age_at_admission'] = np.floor(df['age_at_admission']) #round to nearest year

        # filter out ages below 15 and above 100
        df = df[(df['age_at_admission'] >= 15) & (df['age_at_admission'] <= 100)]
        
        return df

class AgeHistTransform(Transform):
    def input_schema(self):
        return sb.PartialSchema('age', [
            sb.num('age_at_admission')
        ])

    def _transform(self, age_df):
        age_counts = age_df['age_at_admission'].value_counts().sort_index()

        return pd.DataFrame(age_counts, columns=['count'])


