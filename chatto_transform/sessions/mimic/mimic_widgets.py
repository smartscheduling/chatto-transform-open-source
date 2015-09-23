import pandas as pd

import ipywidgets as widgets

from collections import OrderedDict
import os.path

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

def query_text_box():
    return widgets.Textarea(
        description='SQL Query:'
    )

def meditems_multiselect():
    d = os.path.dirname(__file__)
    f = os.path.join(d, 'meditems.csv')
    meditems = pd.read_csv(f)
    meditems = meditems.sort_index(by='name')[['name', 'itemid']]
    m_options = OrderedDict(meditems.to_records(index=False))
    return widgets.SelectMultiple(
        description='Medications',
        options=m_options
    )