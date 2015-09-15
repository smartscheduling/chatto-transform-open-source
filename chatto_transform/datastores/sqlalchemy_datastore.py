import pandas
from chatto_transform.schema.schema_base import *
from chatto_transform.datastores.datastore_base import DataStore
from chatto_transform.datastores.csv_datastore import CsvDataStore
from chatto_transform.datastores.odo_datastore import OdoDataStore

from functools import lru_cache, partial

from sqlalchemy import Table, MetaData, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base

from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Select, and_
from sqlalchemy import sql

import io
import tempfile
import time
import os
import datetime
import odo

metadatas = {}

def get_engine_metadata(engine):
    if engine in metadatas:
        return metadatas[engine]
    else:
        metadata = MetaData()
        metadata.bind = engine
        metadatas[engine] = metadata
        return metadata

def get_reflected_metadata(engine, schema_name=None):
    metadata = MetaData()
    metadata.reflect(bind=engine, schema=schema_name)
    metadata.bind = engine
    return metadata

########################################################################

for col_type in [dt, delta, num, bool_]:
    col_type._storage_target_registry['sqlalchemy'] = col_type._storage_target_registry['pandas'].copy()

@cat.register_check('sqlalchemy')
def _(col):
    return col.dtype == 'object' 

@cat.register_transform('sqlalchemy')
def _(col):
    return col.astype('object')

@id_.register_check('sqlalchemy')
def _(col):
    return col.dtype == 'object'

@id_.register_transform('sqlalchemy')
def _(col):
    return col.astype('object')

########################################################################

@cat.register_metadata('sqlalchemy')
def _(self):
    return sql.schema.Column(self.name, sql.sqltypes.Text, nullable=True)

@id_.register_metadata('sqlalchemy')
def _(self):
    return sql.schema.Column(self.name, sql.sqltypes.Integer, nullable=True)

@dt.register_metadata('sqlalchemy')
def _(self):
    return sql.schema.Column(self.name, sql.sqltypes.DateTime(timezone=True), nullable=True)

@delta.register_metadata('sqlalchemy')
def _(self):
    return sql.schema.Column(self.name, sql.sqltypes.Interval, nullable=True)

@big_dt.register_metadata('sqlalchemy')
def _(self):
    return sql.schema.Column(self.name, sql.sqltypes.DateTime(timezone=True), nullable=True)

@num.register_metadata('sqlalchemy')
def _(self):
    return sql.schema.Column(self.name, sql.sqltypes.Float, nullable=True)

@bool_.register_metadata('sqlalchemy')
def _(self):
    return sql.schema.Column(self.name, sql.sqltypes.Boolean, nullable=True)

########################################################################

@lru_cache()
def schema_as_table(schema, engine):
    if schema.options.get('temporary', False):
        prefixes = ['TEMPORARY']
    else:
        prefixes = []

    db_schema = schema.options.get('db_schema', None)
    metadata = get_engine_metadata(engine)

    return Table(schema.name, metadata, *[col.metadata('sqlalchemy') for col in schema.cols], schema=db_schema, prefixes=prefixes)

sa_type_2_col_type = {
    sql.sqltypes.Integer: num,
    sql.sqltypes.String: cat,
    sql.sqltypes.Date: dt,
    sql.sqltypes.DateTime: dt,
    sql.sqltypes.Interval: delta,
    sql.sqltypes.Numeric: num,
    sql.sqltypes.Boolean: bool_
}

def table_as_schema(table):
    schema_cols = []
    for sa_col in table.c:
        for sa_type, col_type in sa_type_2_col_type.items():
            if isinstance(sa_col.type, sa_type):
                if isinstance(sa_col.type, sql.sqltypes.Integer) and (sa_col.primary_key or sa_col.foreign_keys):
                    schema_cols.append(id_(sa_col.name))
                else:
                    schema_cols.append(col_type(sa_col.name))
                break
    options = {}
    if table.schema is not None:
        options['db_schema'] = table.schema
    s = Schema(table.name, schema_cols, options=options)
    return s

########################################################################

def fast_sql_to_df(table, schema):
    engine = table.bind

    # if engine.dialect.name == 'mysql':
    #    return fast_mysql_to_df(table, schema)
    if engine.dialect.name == 'postgresql':
        return fast_postgresql_to_df(table, schema)

    ods = OdoDataStore(schema, table)
    df = ods.load()
    df = df[schema.col_names()]
    return df

def fast_mysql_to_df(table, schema):
    from chatto_transform.config import config

    f = tempfile.NamedTemporaryFile('w', suffix='.csv', dir=config.data_dir+'tmp')
    try:
        f.close()
        if not isinstance(table, Table):
            compiled = table.compile()   
            table_name = '({})'.format(str(compiled))
            params = [compiled.params[k] for k in compiled.positiontup]
        else:
            table_name = str(table)
            params = []

        # converting to csv
        sql = """SELECT {cols} FROM {table} AS t INTO OUTFILE '{filename}'
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        ESCAPED BY '\\\\'
        LINES TERMINATED BY '\n'""".format(
            cols=', '.join('`'+colname+'`' for colname in schema.col_names()),
            filename=f.name,
            table=table_name)

        table.bind.execute(sql, *params)
        
        # reading csv
        csv_loader = CsvDataStore(schema, f.name, with_header=False, na_values=['\\N'])
        df = csv_loader.load()
        #df = pandas.read_csv(f.name, header=None, names=schema.col_names(), na_values=['\\N'])
    finally:
        os.remove(f.name)

    # for col in schema.cols:
    #     if isinstance(col, dt):
    #         # converting datetime column
    #         df[col.name] = pandas.to_datetime(df[col.name], format="%Y-%m-%d %H:%M:%S", coerce=True)
    #     if isinstance(col, big_dt):
    #         # converting big_dt column
    #         strptime = datetime.datetime.strptime
    #         parse_func = (lambda x: strptime(x, "%Y-%m-%d %H:%M:%S"))
    #         df[col.name] = df[col.name].map(parse_func, na_action='ignore')
    return df

def fast_postgresql_to_df(table, schema):
    engine = table.bind
    conn = engine.raw_connection()
    with conn.cursor() as cur:
        with io.StringIO() as f:
            table_name = str(table)
            if not isinstance(table, Table):
                table_name = '({})'.format(table_name)
            sql = "COPY {table_name} TO STDOUT WITH (FORMAT CSV, HEADER TRUE)".format(
                table_name=table_name)
            cur.copy_expert(sql, f)

            f.seek(0)
            df = pandas.read_csv(f)
            for col in schema.cols:
                if isinstance(col, dt):
                    # converting datetime column
                    df[col.name] = pandas.to_datetime(df[col.name], format="%Y-%m-%d %H:%M:%S", coerce=True)
                if isinstance(col, big_dt):
                    # converting big_dt column
                    strptime = datetime.datetime.strptime
                    parse_func = (lambda x: strptime(x, "%Y-%m-%d %H:%M:%S"))
                    df[col.name] = df[col.name].map(parse_func, na_action='ignore')
    return df

def fast_postgresql_to_csv(table, file_path):
    engine = table.bind
    conn = engine.raw_connection()
    with conn.cursor() as cur:
        with open(file_path, 'w', encoding='utf-8') as f:
            table_name = str(table)
            if not isinstance(table, Table):
                table_name = '({})'.format(table_name)
            sql = "COPY {table_name} TO STDOUT WITH (FORMAT CSV, HEADER TRUE)".format(
                table_name=table_name)
            cur.copy_expert(sql, f)

def fast_df_to_sql(df, table, schema):
    ods = OdoDataStore(schema, table, storage_target_type='sqlalchemy')
    ods.store(df)

class SATableDataStore(DataStore):
    def __init__(self, schema, engine, where_clauses=None):
        super().__init__(schema)
        self.engine = engine 
        self.table = schema_as_table(self.schema, self.engine)
        self.where_clauses = where_clauses

    def storage_target(self):
        return 'sqlalchemy'

    def _load(self):
        query = self.table
        if self.where_clauses is not None:
            query = query.select()
            for where_clause in self.where_clauses:
                query = query.where(where_clause)

        df = fast_sql_to_df(query, self.schema)
        return df

    def to_csv(self, file_path):
        if self.engine.dialect.name != 'postgresql':
            raise NotImplementedError('converting directly to csv not supported for non-postgres databases')
        query = self.table
        if self.where_clauses is not None:
            query = query.select()
            for where_clause in self.where_clauses:
                query = query.where(where_clause)

        fast_postgresql_to_csv(query, file_path)

    def _store(self, df):
        if self.where_clauses is not None:
            raise NotImplementedError('Cannot store to a query (where_clauses must be left blank)')
        df = df.copy()
        fast_df_to_sql(self.table, self.schema)

    def _update(self, df):
        if self.where_clauses is not None:
            raise NotImplementedError('Cannot update to a query (where_clauses must be left blank)')
        df = df.copy()

        with self.engine.connect() as conn:
            temp_schema = Schema.rename(self.schema, 'temp_'+self.schema.name)
            temp_schema.options['temporary'] = True
            temp_table = schema_as_table(temp_schema, self.engine)

            print('storing new df in temp table')
            fast_df_to_sql(df, temp_table, temp_schema)

            print('updating table from matching rows')
            index = self.schema.options['index']
            update = self.table.update(
                values={
                    col_name: temp_table.c[col_name] for col_name in self.schema.col_names()
                },
                whereclause=self.table.c[index] == temp_table.c[index]
            )
            update_res = conn.execute(update)

            print('inserting new rows into table')
            exists_query = self.table.select().where(self.table.c[index] == temp_table.c[index]).exists()

            insert = self.table.insert().from_select(
                temp_schema.col_names(),
                temp_table.select().where(~exists_query))
            ins_res = conn.execute(insert)

    def delete(self):
        if self.where_clauses is not None:
            raise NotImplementedError('Cannot delete a query (where_clauses must be left blank)')
        
        self.table.drop(self.engine)


class SAJoinDataStore(DataStore):
    def __init__(self, root_schema, engine, has_schemas=None, belongs_to_schemas=None, root_conditions=None, where_clauses=None):
        self.engine = engine
        self.root_schema = root_schema
        self.root_table = schema_as_table(self.root_schema, self.engine)
        
        self.has_schemas, self.has_join_conditions = self._parse_schema_list(has_schemas)
        self.has_tables = [schema_as_table(h_schema, self.engine) for h_schema in self.has_schemas]

        self.belongs_to_schemas, self.belongs_to_join_conditions = self._parse_schema_list(belongs_to_schemas)
        self.belongs_to_tables = [schema_as_table(b_schema, self.engine) for b_schema in self.belongs_to_schemas]

        self.root_conditions = root_conditions
        self.where_clauses = where_clauses

        schema = Schema.union([self.root_schema] + self.has_schemas + self.belongs_to_schemas, with_prefix=True, schema_name=self.root_schema.name+'_join')
        super().__init__(schema)

    def _parse_schema_list(self, schema_list=None):
        if schema_list is None:
            schema_list = []
        schemas = []
        join_conditions = {}
        for schema in schema_list:
            if isinstance(schema, tuple):
                schema, j_c = schema
                join_conditions[schema] = j_c
            schemas.append(schema)
        return schemas, join_conditions

    def storage_target(self):
        return 'sqlalchemy'

    def _load(self):
        root = self.root_table
        if self.root_conditions is not None:
            root = root.select().where(and_(*self.root_conditions)).alias()
        join_clause = root

        select_clause = []
        root_col_prefix = self.root_schema.options['prefix']
        for col in root.c:
            select_clause.append(col.label("{}.{}".format(root_col_prefix, col.name)))

        for h_table, h_schema in zip(self.has_tables, self.has_schemas):
            col_prefix = h_schema.options['prefix']
            h_join_conditions = [root.c.id == h_table.c['{}_id'.format(root_col_prefix)]]
            for join_condition in self.has_join_conditions.get(h_schema, []):
                h_join_conditions.append(join_condition)
            join_clause = join_clause.outerjoin(h_table, and_(*h_join_conditions))
            
            for col in h_table.c:
                select_clause.append(col.label("{}.{}".format(col_prefix, col.name)))

        for b_table, b_schema in zip(self.belongs_to_tables, self.belongs_to_schemas):
            col_prefix = b_schema.options['prefix']
            
            b_join_conditions = [root.c['{}_id'.format(col_prefix)] == b_table.c.id]
            for join_condition in self.belongs_to_join_conditions.get(b_schema, []):
                b_join_conditions.append(join_condition)
            join_clause = join_clause.outerjoin(b_table, and_(*b_join_conditions))
            
            for col in b_table.c:
                select_clause.append(col.label("{}.{}".format(col_prefix, col.name)))

        #temp_schema = Schema.rename(self.schema, 'temp_'+self.schema.name)
        #temp_table = schema_as_table(temp_schema, self.engine)        
        #try:
        #    temp_table.create(self.engine)

        query = select(select_clause).select_from(join_clause)
        if self.where_clauses is not None:
            query = query.where(and_(*self.where_clauses))

        #    insert = temp_table.insert().from_select(temp_schema.col_names(), query)

        start = time.time()
        
        print('loading rows from join')
        df = fast_sql_to_df(query, self.schema)
        loaded = time.time()
        #finally:
        #    temp_table.drop(self.engine)
        
        print('took', loaded - start, 'seconds to perform the join and load the results')
        print('type checking and sorting')
        return df

class SAQueryDataStore(DataStore):
    def __init__(self, schema, engine, query):
        self.engine = engine
        self.query = query
        self.schema = schema

    def _load(self):
        df = pandas.read_sql(self.query, self.engine)
        
        return df
