from itertools import chain
import os
import time
from multiprocessing import cpu_count
import sys
import resource
import gc

import pandas
import numpy as np
import joblib

from chatto_transform.transforms.transform_base import Transform
from chatto_transform.lib.chunks import from_chunks, to_chunks, to_group_chunks, CHUNK_SIZE
from chatto_transform.lib import temp_file
from chatto_transform.datastores.hdf_datastore import HdfDataStore

"""Library for executing transforms in parallel.

Splits data into as many groups as there are cpus, and runs a transform on all groups simultaneously.

HDFStore is used as an intermediate storage target, to pass data between jobs."""


class ParallelTransform(Transform):
    def __init__(self, transform, group_index=None, chunksize=None, n_jobs=-1):
        self.transform_obj = transform
        self.group_index = group_index
        if n_jobs == -1:
            n_jobs = cpu_count()
        self.n_jobs = n_jobs
        if chunksize is None:
            chunksize = CHUNK_SIZE
        self.chunksize = chunksize

    def output_schema(self):
        return self.transform_obj.output_schema()

    def input_schema(self):
        return self.transform_obj.input_schema()

    def _transform(self, data):
        start = time.time()
        print('transforming data of size', data.memory_usage(index=True).sum(), 'bytes')
        
        store_chunks_jobs = []
        transform_jobs = []
        hdf_stores = []
        try:
            print('splitting data into large groups')
            group_iter = self._group_iter(data, len(data) // self.n_jobs or self.chunksize)

            for group_data in group_iter:
                if group_data.empty:
                    continue
                f = temp_file.make_temporary_file()
                hdf_store = HdfDataStore(self.input_schema(), f)
                hdf_stores.append(hdf_store)
                hdf_store.store(group_data)

                store_chunks_jobs.append(joblib.delayed(self.store_chunks_job)(hdf_store))
                #transform_jobs.append(joblib.delayed(self.transform_job)(hdf_store))
            print('breaking data into chunks in parallel')
            joblib.Parallel(n_jobs=self.n_jobs)(store_chunks_jobs)

            chunk_stores = chain.from_iterable(store.chunk_stores() for store in hdf_stores)

            transform_jobs = [joblib.delayed(self.transform_job)(chunk_store) for chunk_store in chunk_stores]

            print('running transforms in', len(transform_jobs), 'parallel jobs')
            result_hdf_stores = joblib.Parallel(n_jobs=self.n_jobs)(transform_jobs)

            print('loading and merging the results')
            results = from_chunks(r_hdf_store.load() for r_hdf_store in result_hdf_stores)
            print('finished merge')
        finally:
            for hdf_store in hdf_stores:
                hdf_store.delete_chunks()
                hdf_store.delete()

        end = time.time()
        print('took', end - start, 'seconds to transform all data in parallel')
        return results

    def _group_iter(self, data, chunksize):
        if self.group_index is not None:
            group_iter = to_group_chunks(data, self.group_index, chunksize)
        else:
            group_iter = to_chunks(data, chunksize)
        return group_iter


    def store_chunks_job(self, hdf_store):
        data = hdf_store.load()
        chunks = self._group_iter(data, self.chunksize)
        def gc_chunks():
            for chunk in chunks:
                yield chunk
                del chunk
                gc.collect()
        hdf_store.store_chunks(gc_chunks())
        
    # def transform_job(self, hdf_store):
        


    #     chunks = hdf_store.load_chunks()
    #     transformed_chunks = self.transform_chunks(chunks)
    #     t_hdf_store = HdfDataStore(self.output_schema(), hdf_store.hdf_file)
    #     t_hdf_store.store_chunks(transformed_chunks)
        
    #     return t_hdf_store

    # def transform_chunks(self, chunks):
    #     for chunk in chunks:
    #         yield self.transform_chunk(chunk)
    #         del chunk
    #         gc.collect()

    def transform_job(self, chunk_store):
        gc.collect()
        start = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 10**6 #sys.getallocatedblocks()

        data = chunk_store.load()
        gc.collect()

        finished_load = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 10**6 #sys.getallocatedblocks()

        result = self.transform_chunk(data)
        gc.collect()

        finished_transform = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 10**6 #sys.getallocatedblocks()

        t_hdf_store = HdfDataStore(self.output_schema(), chunk_store.hdf_file)
        t_hdf_store.store(result)
        gc.collect()

        finished_store = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 10**6 #sys.getallocatedblocks()
        print('started with', start, 'mb, ended with', finished_store, 'difference =', finished_store - start)
        print('loading used', finished_load - start, 'mb')
        print('transforming used', finished_transform - finished_load, 'mb')
        print('storing used', finished_store - finished_transform, 'mb')
        return t_hdf_store
            
    def transform_chunk(self, data):
        print('transforming chunk of length', len(data), 'mem usage', data.memory_usage(index=True).sum() / 10**6, 'mb')
        transformed_groups = []
        if self.group_index is None:
            return self.transform_obj.transform(data)
        
        for group_id, group_data in data.groupby(self.group_index, as_index=False, sort=False):
            if group_data.empty: #pandas bug where empty groups are returned when grouped by a category
                continue
            transformed_group = self.transform_obj.transform(group_data)
            transformed_groups.append(transformed_group)

        transformed_data = pandas.concat(transformed_groups)
        return transformed_data

