import pandas
from collections import defaultdict
from functools import reduce

def to_chunks(df, chunksize=1048576):
    indexes = list(range(0, len(df) + chunksize, chunksize))
    for start, stop in zip(indexes, indexes[1:]):
        chunk = df.iloc[start:stop]
        if chunk.empty:
            break
        yield chunk

def from_chunks(chunks):
    dtype_chunks = defaultdict(list)
    column_category_chunks = defaultdict(list)
    for chunk in chunks:
        chunk_numbers = chunk.select_dtypes(include=['float', 'int'])
        chunk[chunk_numbers.columns] = chunk_numbers.astype('float64')
        for dtype in map(str, chunk.dtypes.unique()):
            dtype_chunk = chunk.select_dtypes(include=[dtype])
            if dtype == 'category':
                for col in dtype_chunk.columns:
                    column_category_chunks[col].append(dtype_chunk[col].cat.categories)
            dtype_chunks[dtype].append(dtype_chunk)

    combined_dtypes = {}
    combined_column_categories = {}
    for dtype, d_chunks in dtype_chunks.items():
        if dtype == 'category':
            for column, category_chunks in column_category_chunks.items():
                combined_categories = reduce(pandas.Index.union, category_chunks)
                combined_column_categories[column] = combined_categories
                for cat_chunk in d_chunks:
                    cat_chunk[column] = cat_chunk[column].cat.set_categories(combined_categories).cat.codes
        combined_dtype = pandas.concat(d_chunks, copy=False, ignore_index=True)
        combined_dtypes[dtype] = combined_dtype

    df = pandas.concat(combined_dtypes.values(), axis=1, copy=False)

    for column, combined_categories in combined_column_categories.items():
        df[column] = pandas.Categorical.from_codes(df[column], combined_categories, name=column)

    return df
