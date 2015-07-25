from ..schema.schema_base import *
import tempfile
import os
import stat

from .datastore_base import DataStore

from psycopg2.extras import Json as psycopg2_json
import time
import io

def psql_name_to_pandas(col_name):
    return col_name.replace('__', '.')

def pandas_name_to_psql(col_name):
    return col_name.replace('.', '__')

def psql_rename_schema(schema):
    psql_schema = Schema.rename(schema, pandas_name_to_psql(schema.name),
        rename_cols={col: pandas_name_to_psql(col) for col in schema.col_names()})
    if 'index' in psql_schema.options:
        psql_schema.options['index'] = pandas_name_to_psql(psql_schema.options['index'])
    if 'order_by' in psql_schema.options:
        psql_schema.options['order_by'] = [pandas_name_to_psql(col) for col in psql_schema.options['order_by']]
        
    return psql_schema

##################################################################################

class json_data(Column):
    pass

##################################################################################

for col_type in [cat, id_, dt, delta, num, bool_]:
    col_type._storage_target_registry['psql'] = col_type._storage_target_registry['pandas'].copy()

@cat.register_check('psql')
def _(col):
    return col.dtype == 'object'

@cat.register_transform('psql')
def _(col):
    return col.astype('object')

@id_.register_check('psql')
def _(col):
    return col.dtype == 'float64'
    
@id_.register_transform('psql')
def _(col):
    col = col.astype('float64')
    return col

##################################################################################

@cat.register_metadata('psql')
def _(self):
    return '"{}" VARCHAR(255)'.format(pandas_name_to_psql(self.name))

@id_.register_metadata('psql')
def _(self):
    return '"{}" INT'.format(pandas_name_to_psql(self.name))

@dt.register_metadata('psql')
def _(self):
    return '"{}" TIMESTAMP'.format(pandas_name_to_psql(self.name))

@delta.register_metadata('psql')
def _(self):
    return '"{}" INTERVAL'.format(pandas_name_to_psql(self.name))

@num.register_metadata('psql')
def _(self):
    return '"{}" NUMERIC'.format(pandas_name_to_psql(self.name))

@bool_.register_metadata('psql')
def _(self):
    return '"{}" NUMERIC'.format(pandas_name_to_psql(self.name)) #storing as numeric for pandas compatibility

@json_data.register_metadata('psql')
def _(self):
    return '"{}" JSON'.format(self.name)

##################################################################################

for col_type in [id_, dt, delta, num, bool_]:
    col_type._storage_target_registry['psql_encoded'] = col_type._storage_target_registry['psql'].copy()

@cat.register_check('psql_encoded')
def _(col):
    return col.dtype == 'int64'

@cat.register_transform('psql_encoded')
def _(col):
    return col.cat.codes

@cat.register_metadata('psql_encoded')
def _(self):
    return '"{}" INT'.format(pandas_name_to_psql(self.name))

##################################################################################

table_categories_schema = Schema('table_categories', [
    cat('table_name'),
    json_data('categories')
])

def get_df_categories(df, schema):
    col_categories = {}
    for col in schema.cols:
        if isinstance(col, cat):
            col_categories[col.name] = df[col.name].cat.categories.tolist()
    return col_categories

def load_table_categories(name, engine):
    if not engine.has_table('table_categories'):
        create_table(engine, table_categories_schema)

    query = """SELECT * from table_categories WHERE table_name = '{}'""".format(name)
    result = engine.execute(query)
    row = result.first()
    if row is None:
        return {}
    return row[1]


def store_table_categories(name, categories, engine):
    if not engine.has_table('table_categories'):
        create_table(engine, table_categories_schema)
    
    update = """UPDATE table_categories SET categories = %s WHERE table_name = %s"""
    result = engine.execute(update, psycopg2_json(categories), name)
    if not result.rowcount:
        insert = """INSERT INTO table_categories (table_name, categories) VALUES (%s, %s)"""
        engine.execute(insert, name, psycopg2_json(categories))

def merge_categories(cat1, cat2):
    merged = {}
    for col in (cat1.keys() | cat2.keys()):
        c1 = cat1.get(col, [])
        c2 = cat2.get(col, [])
        new = set(c2).difference(set(c1))

        merged[col] = c1 + list(new)
    return merged

def update_df_categories(df, categories):
    df = df.copy()
    for col in categories:
        df[col] = df[col].cat.set_categories(categories[col])
    return df

def hydrate_categories(df, categories):
    df = df.copy()
    for col in categories:
        codes = df[col].astype('float64').fillna(-1).astype('int64')
        df[col] = pandas.Categorical.from_codes(codes, categories=categories[col], name=col)
    return df

def fast_df_to_csv(df, f, schema):
    start = time.time()
    df = df.copy()
    df.to_csv(f, index=False, date_format="%Y-%m-%d %H:%M:%S")
    end = time.time()
    print('took', end - start, 'seconds to convert to csv')

def df_chunks(df, chunksize=65536):
    steps = list(range(0, len(df), chunksize))
    if steps[-1] != len(df):
        steps.append(len(df))
    for start, stop in zip(steps, steps[1:]):
        yield df.iloc[start:stop]

def fast_df_to_sql(df, engine, psql_schema):
    convert_id_columns_to_type(engine, psql_schema, 'NUMERIC')
    conn = engine.raw_connection()
    with conn.cursor() as cur:
        with io.StringIO() as f:
            print('converting df to csv')
            fast_df_to_csv(df, f, psql_schema)
            
            print('copying csv into db')
            f.seek(0)
            sql = "COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)".format(
                table_name=psql_schema.name)
            cur.copy_expert(sql, f)
    conn.commit()
    convert_id_columns_to_type(engine, psql_schema, 'INT')

def convert_id_columns_to_type(engine, psql_schema, new_type):
    query = """ALTER TABLE {table} {alter_cols}"""
    alter_cols = []
    for col in psql_schema.cols:
        if isinstance(col, id_):
            print('converting column', col.name, 'to', new_type)
            alter_col_sql = 'ALTER COLUMN "{col}" TYPE {type}'.format(col=col.name, type=new_type)
            alter_cols.append(alter_col_sql)
    alter_cols_sql = ', '.join(alter_cols)
    engine.execute(query.format(table=psql_schema.name, alter_cols=alter_cols_sql))

def fast_sql_to_df(engine, psql_schema):
    conn = engine.raw_connection()
    with conn.cursor() as cur:
        with io.StringIO() as f:
            start = time.time()
            print('copying table to csv')
            sql = "COPY {table_name} TO STDOUT WITH (FORMAT CSV, HEADER TRUE)".format(
                table_name=psql_schema.name)
            cur.copy_expert(sql, f)

            print('reading csv into df')
            f.seek(0)
            df = pandas.read_csv(f)
            print('converting date columns')
            for col in psql_schema.cols:
                if isinstance(col, dt):
                    print('converting datetime column', col.name)
                    df[col.name] = pandas.to_datetime(df[col.name], format="%Y-%m-%d %H:%M:%S", coerce=True)
            end = time.time()
            print('finished loading table in', end - start, 'seconds')
    return df

def create_table(engine, psql_schema, encode_categoricals=True):
    if engine.has_table(psql_schema.name):
        drop_table(engine, psql_schema)
    storage_target = 'psql_encoded' if encode_categoricals else 'psql'
    column_sql = ', '.join(col.metadata(storage_target) for col in psql_schema.cols)
    engine.execute('CREATE TABLE "{}" ({})'.format(psql_schema.name, column_sql))

def create_indexes(engine, psql_schema):
    index = psql_schema.options.get('index', None)
    if index is not None:
        index_col = pandas_name_to_psql(index)
        engine.execute('CREATE INDEX ON "{}" ("{}")'.format(psql_schema.name, index_col))

    sort = psql_schema.options.get('order_by')
    if sort is not None:
        if isinstance(sort, str):
            sort = [sort]
        for s in sort:
            s_col = pandas_name_to_psql(s)
            engine.execute('CREATE INDEX ON "{}" ("{}" DESC)'.format(psql_schema.name, s_col))

def drop_table(engine, psql_schema):
    engine.execute('DROP TABLE "{}"'.format(psql_schema.name))

class PSqlDataStore(DataStore):
    def __init__(self, schema, engine, encode_categoricals=True, load_where_conditions=None):
        self.engine = engine
        self.encode_categoricals = encode_categoricals
        self.load_where_conditions = load_where_conditions
        super().__init__(schema)
        self.psql_schema = psql_rename_schema(self.schema)

    def _load(self):
        if self.load_where_conditions is not None:
            temp_name = 't_'+self.psql_schema.name
            temp_schema = Schema.rename(self.psql_schema, temp_name)
            try:
                print('creating psql temp table and loading select into it')
                create_table(self.engine, temp_schema, encode_categoricals=self.encode_categoricals)

                cols = ", ".join('"{col_name}"'.format(col_name=col_name) for col_name in temp_schema.col_names())
                where_conditions = ' AND '.join(self.load_where_conditions)

                insert_query = """INSERT INTO {temp} ({cols}) (
                    SELECT * FROM {table} WHERE {where_conditions}
                )""".format(temp=temp_schema.name, cols=cols, table=self.psql_schema.name, where_conditions=where_conditions)

                self.engine.execute(insert_query)

                df = fast_sql_to_df(self.engine, temp_schema)
            finally:
                drop_table(self.engine, temp_schema)
        else:    
            df = fast_sql_to_df(self.engine, self.psql_schema)

        if self.encode_categoricals:
            categories = load_table_categories(self.psql_schema.name, self.engine)
            df = hydrate_categories(df, categories)

        df = df[self.psql_schema.col_names()]
        df.columns = self.schema.col_names()
        return df

    def _store(self, df):
        """Store df to table in the db. If the table already exists it is replaced."""
        start = time.time()
        df = df.copy()
        
        if self.engine.has_table(self.psql_schema.name):
            drop_table(self.engine, self.psql_schema)
        
        df = df[self.schema.col_names()]
        df.columns = self.psql_schema.col_names()

        if self.encode_categoricals:
            categories = get_df_categories(df, self.psql_schema)
            store_table_categories(self.psql_schema.name, categories, self.engine)

        create_table(self.engine, self.psql_schema, encode_categoricals=self.encode_categoricals)
        fast_df_to_sql(df, self.engine, self.psql_schema)
        create_indexes(self.engine, self.psql_schema)
        end = time.time()
        print('took', end - start, 'seconds to store df in postgres and create indexes on it')
            
    def _update(self, new_df):
        start = time.time()
        new_df = new_df[self.schema.col_names()]
        new_df.columns = self.psql_schema.col_names()

        if not self.engine.has_table(self.psql_schema.name):
            self._store(new_df, index, sort)
            return

        if self.encode_categoricals:
            old_categories = load_table_categories(self.psql_schema.name, self.engine)
            new_categories = get_df_categories(new_df, self.psql_schema)
            merged_categories = merge_categories(old_categories, new_categories)
            new_df = update_df_categories(new_df, merged_categories)
            store_table_categories(self.psql_schema.name, merged_categories, self.engine)
        
        temp_name = 't_'+self.psql_schema.name
        temp_schema = Schema.rename(self.psql_schema, temp_name)
        
        try:
            print('creating psql temp table and loading new df into it')
            create_table(self.engine, temp_schema, encode_categoricals=self.encode_categoricals)
            fast_df_to_sql(new_df, self.engine, temp_schema)
            
            temp_tab_loaded = time.time()

            print('indexing temp table for fast update with existing')
            create_indexes(self.engine, temp_schema)

            temp_indexes_created = time.time()

            print('updating existing table with rows from temp')
            set_exprs = []
            for col in self.psql_schema.col_names():
                set_expr = '"{col}" = "{temp}"."{col}"'.format(
                    temp=temp_schema.name, col=col)
                set_exprs.append(set_expr)

            index = self.psql_schema.options['index']
            table = self.psql_schema.name
            temp = temp_schema.name
            set_clause = 'SET ' + ', '.join(set_exprs)
            update = '''UPDATE "{table}" {set_clause}
                FROM "{temp}" WHERE "{table}"."{index}" = "{temp}"."{index}"'''.format(
                set_clause=set_clause,
                table=table,
                temp=temp,
                index=index)
            
            update_res = self.engine.execute(update)

            tab_updated = time.time()

            print('inserting new rows into existing table from temp')
            insert = '''INSERT INTO "{table}"
                SELECT * FROM "{temp}" WHERE NOT EXISTS (SELECT * FROM "{table}" WHERE "{table}"."{index}" = "{temp}"."{index}")'''.format(
                table=table,
                temp=temp,
                index=index)

            insert_res = self.engine.execute(insert)

            tab_inserted = time.time()

            print('took', temp_tab_loaded - start, 'seconds to create and load temp table with', len(new_df), 'rows')
            print('took', temp_indexes_created - temp_tab_loaded, 'seconds to index temp table')
            print('took', tab_updated - temp_indexes_created, 'seconds to update', update_res.rowcount, 'existing rows from temp')
            print('took', tab_inserted - tab_updated, 'seconds to insert', insert_res.rowcount, 'new rows into table')
        finally:
            drop_table(self.engine, temp_schema)

    def delete(self):
        if self.engine.has_table(self.psql_schema.name):
            drop_table(self.engine, self.schema)



