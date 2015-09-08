import pandas as pd
from collections import defaultdict
from functools import reduce

CHUNK_SIZE = 262144

def to_chunks(df, chunksize=CHUNK_SIZE):
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
        chunk_numbers = chunk.select_dtypes(include=['float', 'int', 'bool'])
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
                combined_categories = reduce(pd.Index.union, category_chunks)
                combined_column_categories[column] = combined_categories
                for cat_chunk in d_chunks:
                    cat_chunk[column] = cat_chunk[column].cat.set_categories(combined_categories).cat.codes
        combined_dtype = pd.concat(d_chunks, copy=False, ignore_index=True)
        combined_dtypes[dtype] = combined_dtype

    df = pd.concat(combined_dtypes.values(), axis=1, copy=False)

    for column, combined_categories in combined_column_categories.items():
        df[column] = pd.Categorical.from_codes(df[column], combined_categories, name=column)

    return df

def to_group_chunks(df, column, chunksize=CHUNK_SIZE, group_rows_hint=None):
    groups = pd.Series(df[column].unique())

    if group_rows_hint is None:
        group_rows_hint = len(df) // len(groups) * 100 

    group_stack = []
    for g_ids in to_chunks(groups, chunksize=chunksize // group_rows_hint or 1):
        group_stack.extend(g_ids)
        chunk_mask = df[column].isin(group_stack)
        chunk_len = chunk_mask.sum()
        if chunk_len >= chunksize:
            yield df[chunk_mask]
            group_stack = []
    if group_stack:
        chunk_mask = df[column].isin(group_stack)
        yield df[chunk_mask]

def left_join(left_df, right_df, on=None, left_on=None, right_on=None):
    """Wrapper around `pandas.merge`, but converts categoricals to and from codes to speed up the merge
    """
    if on is not None:
        if left_on is not None or right_on is not None:
            raise TypeError('must specify either on or both left_on and right_on')
    elif left_on is None or right_on is None:
        raise TypeError('must specify either on or both left_on and right_on')


    cat_categories = {}
    for chunk in [left_df,  right_df]:
        cat_cols = chunk.select_dtypes(include=['category']).columns
        for col in cat_cols:
            cat_categories[col] = chunk[col].cat.categories
            chunk[col] = chunk[col].cat.codes
    
    if on is not None:
        df = pd.merge(left_df, right_df, how='left', on=on, sort=False, copy=False)
    else:
        df = pd.merge(left_df, right_df, how='left',
            left_on=left_on, right_on=right_on, sort=False, copy=False)

    for column, categories in cat_categories.items():
        df[column] = pd.Categorical.from_codes(df[column].fillna(-1), categories, name=column)

    return df

def horizontal_merge(chunks):
    cat_chunks = []
    cat_categories = {}
    other_chunks = []
    for chunk in chunks:
        cat_chunk = chunk.select_dtypes(include=['category'])
        for col in cat_chunk.columns:
            cat_categories[col] = cat_chunk[col].cat.categories
            cat_chunk[col] = cat_chunk[col].cat.codes
        cat_chunks.append(cat_chunk)
        
        other_chunks.append(chunk.select_dtypes(exclude=['category']))

    df = pd.concat(cat_chunks + other_chunks, axis=1, copy=False)

    for column, categories in cat_categories.items():
        df[column] = pd.Categorical.from_codes(df[column], categories, name=column)

    return df





