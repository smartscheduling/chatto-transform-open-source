from itertools import zip_longest, islice
from tempfile import mkstemp
import os
from contextlib import contextmanager, suppress
import time
import random
import string

import pandas
from sklearn.externals import joblib

from ...config import config
from ...schema.schema_base import Schema
from ..transform_base import Transform

"""Library for executing transforms in parallel.

Uses a MapReduce-like approach to parallelizing a transform, splitting the data
repeatedly and then transforming the atomic chunks.

HDFStore is used as an intermediate storage target, to pass data between jobs."""


class PrepareJobResult:
    def __init__(self, job_type, jobs):
        self.job_type = job_type
        self.jobs = jobs

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)

def expanding_groups(iterable, min_size=1):
    values = list(iterable)
    if min_size > len(values):
        min_size = len(values)
    for l in range(min_size, len(values) + 1):
        yield values[:l]

class ParallelTransform(Transform):
    def __init__(self, transform, group_index, split_bins=3, min_rows_split=1024, group_rows_hint=None, n_jobs=-1):
        self.transform_obj = transform
        self.group_index = group_index
        self.split_bins = split_bins
        self.min_rows_split = min_rows_split
        self.n_jobs = n_jobs
        if group_rows_hint is None:
            group_rows_hint = min_rows_split
        self.group_rows_hint = group_rows_hint

    def output_schema(self):
        return self.transform_obj.output_schema()

    def input_schema(self):
        return self.transform_obj.input_schema()

    def _transform(self, data):
        print('splitting data into groups')
        transform_jobs_result = self.split_and_prepare_transform_jobs(data)
        print('transforming data in', len(transform_jobs_result.jobs), 'groups')
        results = joblib.Parallel(n_jobs=self.n_jobs)(transform_jobs_result.jobs)

        transformed = self.merge_results(results)
        return transformed

    def load_and_merge_results_job(self, results):
        transformed = self.load_and_merge_results(results)
        return transformed

    def _update_split_transform_lists(self, pjr, split_jobs, transform_jobs):
        if pjr.job_type == 'split':
            split_jobs.extend(pjr.jobs)
        elif pjr.job_type == 'transform':
            transform_jobs.extend(pjr.jobs)

    def split_and_prepare_transform_jobs(self, data):
        pjr = self.prepare_jobs(data)

        split_jobs = []
        transform_jobs = []
        self._update_split_transform_lists(pjr, split_jobs, transform_jobs)

        while split_jobs:
            pjrs = joblib.Parallel(n_jobs=self.n_jobs)(split_jobs)
            split_jobs = []
            for pjr in pjrs:
                self._update_split_transform_lists(pjr, split_jobs, transform_jobs)
        return PrepareJobResult('transform', transform_jobs)

    def prepare_jobs(self, data, disable_splits=False):
        ids = list(data[self.group_index].unique())

        if len(data) < self.min_rows_split or len(ids) < self.split_bins * self.min_rows_split:
            return self.prepare_transform_jobs(data, ids)
        else:
            return self.prepare_split_jobs(data, ids)
        
    def prepare_split_jobs(self, data, ids):
        ids = set(ids)
        id_groups = list(map(list, grouper(ids, (len(ids) // self.split_bins) + 1)))

        print('splitting', len(ids), 'atoms into', len(id_groups), 'groups')
            
        split_jobs = []
        for id_group in id_groups:
            group_data = data[data[self.group_index].isin(id_group)]
            split_jobs.append(joblib.delayed(self.prepare_jobs)(group_data))
        return PrepareJobResult('split', split_jobs)

    def prepare_transform_jobs(self, data, ids):
        id_groups = []
        ids = set(ids)
        total_ids = len(ids)
        while ids:
            all_data = data[data[self.group_index].isin(ids)]
            if len(all_data) < self.min_rows_split or len(ids) == 1:
                id_groups.append(list(ids))
                break
            for id_group in expanding_groups(ids, self.min_rows_split // self.group_rows_hint):
                group_data = data[data[self.group_index].isin(id_group)]
                if len(group_data) >= self.min_rows_split:
                    id_groups.append(id_group)
                    ids = ids - set(id_group)
                    break
        
        print('preparing transfornms for', total_ids, 'atoms in', len(id_groups), 'groups')
        transform_jobs = []
        for id_group in id_groups:
            group_data = data[data[self.group_index].isin(id_group)]
            transform_jobs.append(joblib.delayed(self.transform_job)(group_data))

        return PrepareJobResult('transform', transform_jobs)

    def transform_job(self, data):
        start = time.time()
        transformed_groups = []
        for group_id, group_data in data.groupby(self.group_index, as_index=False):
            if group_data.empty: #pandas bug where empty groups are returned when grouped by a category
                continue
            transformed_group = self.transform_obj.transform(group_data)
            transformed_groups.append(transformed_group)

        transformed_data = pandas.concat(transformed_groups)

        end = time.time()
        print('finished transform job in', end - start, 'seconds')
        return transformed_data

    def merge_results(self, results):
        while len(results) > self.split_bins:
            print('merging', len(results), 'pieces')
            merge_jobs = self.prepare_merge_jobs(results)
            results = joblib.Parallel(n_jobs=self.n_jobs)(merge_jobs)
        return self.load_and_merge_results(results)

    def prepare_merge_jobs(self, results):
        result_groups = grouper(results, self.split_bins)
        merge_jobs = []
        for result_group in result_groups:
            result_group = list(result_group)
            merge_jobs.append(joblib.delayed(self.load_and_merge_results_job)(result_group))
        return merge_jobs

    def load_and_merge_results(self, results):
        transformed_list = []
        for transformed_data in results:
            if transformed_data is not None: #grouper() will pad groups with None if not enough
                transformed_list.append(transformed_data)

        transformed = pandas.concat(transformed_list)

        return transformed
