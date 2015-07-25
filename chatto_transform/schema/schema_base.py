import pandas
import datetime
import dateutil
import numpy
import copy
from collections import defaultdict

pandas.options.mode.use_inf_as_null = True

class Schema:
    """Schema class. Implements a basic DSL for defining the types and columns of a given dataframe.
    A dataframe is assumed to have a particular set of columns, each with a type and name.
    Chatto-Transform supports multiple storage media for dataframes, and the Schema DSL provides the glue between storage media.
    The default storage medium is pandas DataFrames. These are in-memory high-density tables with a rich DSL and toolset for
    performing fast transformations and queries.

    The base column types supported by Chatto-Transform are heavily influenced by pandas and its idioms."""
    def __init__(self, name, cols, options=None):
        self.cols = list(cols)
        self.name = name
        if options is None:
            options = {}
        else:
            options = options.copy()
        # typecheck options we know about
        if 'order_by' in options:
            if not isinstance(options['order_by'], list):
                raise TypeError('order_by option must be a list of column names. Got '+type(options['order_by']))
        if 'index' in options:
            if not isinstance(options['index'], str):
                raise TypeError('index option must be a string column name. Got ', options['index'])

        self.options = options

    @classmethod
    def union(cls, schemas, with_prefix=False, drop=None, add=None, rename=None, schema_name=None, options=None):
        if drop is None:
            drop = []
        unique_names = set()
        new_cols = []
        if rename is None:
            rename = []
        for schema in schemas:
            for col in schema.cols:
                name = col.name if isinstance(col, Column) else col
                if with_prefix:
                    name = "{}.{}".format(schema.options['prefix'], name)
                if name in unique_names:
                    continue #should really throw an exception here
                if name in drop:
                    continue
                if name in rename:
                    name = rename[name]
                if isinstance(col, Column):
                    new_col = copy.copy(col)
                    new_col.name = name
                else:
                    new_col = name
                new_cols.append(new_col)
                unique_names.add(name)
        if add is not None:
            new_cols.extend(add)

        if schema_name is None:
            schema_name = 'union_' + '_'.join(schema.name for schema in schemas)
        return cls(schema_name, new_cols, options)

    @classmethod
    def rename(cls, schema, new_name=None, rename_cols=None, options=None):
        new_cols = []
        if rename_cols is None:
            rename_cols = {}
        for old_col in schema.cols:
            new_col = copy.copy(old_col)
            if old_col.name in rename_cols:
                new_col.name = rename_cols[old_col.name]
            new_cols.append(new_col)
        if new_name is None:
            new_name = schema.name
        if options is None:
            options = {}
        new_options = schema.options.copy()
        new_options.update(options)
        new_schema = cls(new_name, new_cols, new_options)

        return new_schema

    def copy(self):
        return copy.deepcopy(self)

    def col_names(self, cols=None):
        if cols is None:
            cols = self.cols
        return [col.name if isinstance(col, Column) else col for col in cols]

    def conform_df(self, df, storage_target='pandas', skip_sort=False, add_prefix=False):
        if set(df.columns) != set(self.col_names()):
            sym_diff = set(df.columns) ^ set(self.col_names())
            raise TypeError('df columns do not match schema. non-matching were: {}'.format(sym_diff))

        for col in self.cols:
            if isinstance(col, Column):
                col.conform(df, storage_target=storage_target)

        if not skip_sort and self.options.get('order_by', False):
            order_by_cols = self.options['order_by']
            sort_index = pandas.MultiIndex.from_arrays([df[col] for col in order_by_cols])
            if not sort_index.is_monotonic: #check if it is sorted already
                df.reset_index(inplace=True) #save the current index in column 'index'
                df.index = sort_index
                df.sort_index(inplace=True)
                df.set_index('index', drop=True, inplace=True)

        if add_prefix:
            self.add_prefix(df)

    def add_prefix(self, df):
        df.columns = [self.name+'.'+col for col in df.columns]

    def __eq__(self, other_schema):
        return type(self) == type(other_schema) and self.cols == other_schema.cols and self.name == other_schema.name

    def __hash__(self):
        return hash(type(self)) + hash(tuple(self.cols)) + hash(self.name)

    def __repr__(self):
        col_members = ',\n    '.join(repr(col) for col in self.cols)
        cols = ', [\n    {cols}\n]'.format(cols=col_members)
        if self.options:
            option_members = ',\n    '.join('{k}: {v}'.format(k=repr(k), v=repr(v))
                                            for k, v in self.options.items())
            options = ',\noptions={{\n    {options}\n}}'.format(options=option_members)  
        else:
            options = ''
        return '''Schema("{name}"{cols}{options})'''.format(name=self.name, cols=cols, options=options)

class PartialSchema(Schema):
    def __init__(self, name=None, cols=None, options=None):
        if name is None:
            name = 'anon_partial_schema'
        if cols is None:
            cols = []
        super().__init__(name, cols, options)

    def conform_df(self, df, storage_target='pandas', skip_sort=False, add_prefix=False):
        if not set(df.columns) >= set(self.col_names()):
            missing = set(self.col_names()) - set(df.columns) 
            raise TypeError('some columns missing form partial schema: {}'.format(missing))

        for col in self.cols:
            if isinstance(col, Column):
                col.conform(df, storage_target=storage_target)

        if not skip_sort and self.options.get('order_by', False):
            order_by_cols = self.options['order_by']
            sort_index = pandas.MultiIndex.from_arrays([df[col] for col in order_by_cols])
            if not sort_index.is_monotonic: #check if it is sorted already
                df.reset_index(inplace=True) #save the current index in column 'index'
                df.index = sort_index
                df.sort_index(inplace=True)
                df.set_index('index', drop=True, inplace=True)

        if add_prefix:
            self.add_prefix(df)

class MultiSchema:
    def __init__(self, schema_dict):
        if not isinstance(schema_dict, dict) or len(schema_dict) == 0:
            raise TypeError('schema_dict must be a dict')
        self.schema_dict = schema_dict.copy()

    def conform_df(self, data, storage_target='pandas', skip_sort=False, add_prefix=False):
        if not isinstance(data, dict):
            raise TypeError('MultiSchema conform_df() expects a dictionary of name:df. Got {}'.format(data))

        if data.keys() != self.schema_dict.keys():
            sym_diff = data.keys() ^ self.schema_dict.keys()
            raise TypeError('data dictionary keys do not match multischema. non-matching were: {}'.format(sym_diff))

        for key, df in data.items():
            self[key].conform_df(df, storage_target, skip_sort, add_prefix)

    def copy(self):
        return copy.deepcopy(self)

    def __eq__(self, other_multischema):
        return set(self.schema_dict.items()) == set(other_multischema.schema_dict.items())

    def __hash__(self):
        return hash(frozenset(self.schema_dict.items()))

    def __repr__(self):
        schema_k_v = ['{k}: {v}'.format(k=repr(k), v=repr(v)) for k, v in self.schema_dict.items()]
        k_v_repr = '\n, '.join(schema_k_v)
        return '''MultiSchema({{\n{schemas}\n}}))'''.format(schemas=k_v_repr)

    def __getitem__(self, name):
        return self.schema_dict[name]

class ColumnMeta(type):
    def __new__(mcls, name, bases, dct):
        dct['_storage_target_registry'] = defaultdict(dict)
        return type.__new__(mcls, name, bases, dct)

class Column(metaclass=ColumnMeta):
    def __init__(self, name):
        self.name = name

    def check(self, col, storage_target='pandas'):
        #print('checking column', self.name, 'for type', self.__class__.__name__)
        return self._get_storage_target(storage_target, 'check')(col)
        
    def transform(self, col, storage_target='pandas'):
        #print('transforming column', self.name, 'for type', self.__class__.__name__)
        return self._get_storage_target(storage_target, 'transform')(col)
        
    def conform(self, df, storage_target='pandas'):
        col = df[self.name]
        if not self.check(col, storage_target):
            df[self.name] = self.transform(col, storage_target)

    def __eq__(self, other_col):
        return type(self) == type(other_col) and self.name == other_col.name

    def __hash__(self):
        return hash(type(self)) + hash(self.name)

    def __repr__(self):
        return '{type}("{name}")'.format(type=self.__class__.__name__, name=self.name)

    def metadata(self, storage_target):
        return self._get_storage_target(storage_target, 'metadata')(self)

    @classmethod
    def register_check(cls, *target_names):
        return cls._register_decorator(target_names, 'check')

    @classmethod
    def register_transform(cls, *target_names):
        return cls._register_decorator(target_names, 'transform')

    @classmethod
    def register_metadata(cls, *target_names):
        return cls._register_decorator(target_names, 'metadata')

    @classmethod
    def _register_decorator(cls, target_names, handler_name):
        if handler_name not in ('check', 'transform', 'metadata'):
            raise NameError('Cannot register a storage target named {}'.format(handler_name))

        def _decorator(handler):
            for target_name in target_names:
                cls._storage_target_registry[target_name][handler_name] = handler
            return handler
        return _decorator

    @classmethod
    def _get_storage_target(cls, storage_target, handler_name):
        column_type_name = cls.__name__
        if storage_target not in cls._storage_target_registry:
            raise TypeError('Storage target {} not registered for column type {}'.format(storage_target, column_type_name))
        if handler_name not in cls._storage_target_registry[storage_target]:
            raise TypeError('Storage target {} missing {} handler for column type {}'.format(storage_target, handler_name, column_type_name))
        return cls._storage_target_registry[storage_target][handler_name]

###############################################################################

class cat(Column):
    pass

@cat.register_check('pandas')
def _(col):
    return hasattr(col, 'cat')
    
@cat.register_transform('pandas')
def _(col):
    return col.astype('category')

###############################################################################

class id_(Column):
    pass

@id_.register_check('pandas')
def _(col):
    return hasattr(col, 'cat') and col.cat.categories.dtype in ['object', 'int64']

@id_.register_transform('pandas')
def _(col):
    col = col.astype('category')
    if col.cat.categories.dtype == 'float64':
        col = col.cat.set_categories(col.cat.categories.map(int))
    return col
    
###############################################################################

class dt(Column):
    pass

@dt.register_check('pandas')
def _(col):
    return col.dtype == 'datetime64[ns]'

@dt.register_transform('pandas')
def _(col):
    return pandas.to_datetime(col, coerce=True, infer_datetime_format=True).fillna(pandas.NaT).astype('datetime64[ns]')

###############################################################################

class delta(Column):
    pass

@delta.register_check('pandas')
def _(col):
    return col.dtype == 'timedelta64[ns]'

@delta.register_transform('pandas')
def _(col):
    return pandas.to_timedelta(col)

###############################################################################

class period(Column):
    pass

@period.register_check('pandas')
def _(col):
    return col.dtype == 'object'

@period.register_transform('pandas')
def col_to_period(col):
    import pdb; pdb.set_trace()
    return col.map(dateutil.parser.parse, na_action='ignore')

###############################################################################


class num(Column):
    pass

@num.register_check('pandas')
def _(col):
    return col.dtype == 'float64'

@num.register_transform('pandas')
def _(col):
    return col.astype('float64')

###############################################################################

class bool_(Column):
    pass

@bool_.register_check('pandas')
def _(col):
    if col.dtype != 'float64':
        return False
    if not set(col.dropna().unique()) <= {1.0, 0.0}:
        raise TypeError("bool_ column has value other than nan, 1 or 0.")
    return True

@bool_.register_transform('pandas')
def _(col):
    return col.astype('float64')

###############################################################################

class obj(Column):
    pass

@obj.register_check('pandas')
def _(col):
    return col.dtype == 'object'

@obj.register_transform('pandas')
def _(col):
    return col.astype('object')
