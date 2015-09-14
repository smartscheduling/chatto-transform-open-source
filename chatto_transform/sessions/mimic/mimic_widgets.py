import ipywidgets as widgets

from collections import OrderedDict

from chatto_transform.schema.mimic import mimic_schema

from chatto_transform.schema.schema_base import Schema

def schema_select():
    s_options = OrderedDict()
    for name in sorted(dir(mimic_schema)):
        s = getattr(mimic_schema, name)    
        if isinstance(s, Schema):
            s_options[s.name] = s

    s_select = widgets.Select(
        description='Table:',
        options=s_options
    )

    return s_select

def where_clause_text():
    return widgets.Text(
        description='Where:'
    )