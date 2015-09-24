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
    w = widgets.SelectMultiple(
        description='Medications',
        options=m_options
    )

    @w.on_displayed
    def on_displayed(w):
        w.selected_labels = [meditems.ix[0, 'name']]

    return w

def labitems_multiselect():
    d = os.path.dirname(__file__)
    f = os.path.join(d, 'labitems.csv')
    labitems = pd.read_csv(f)
    labitems = labitems.sort_index(by='name')[['name', 'itemid']]
    l_options = OrderedDict(labitems.to_records(index=False))
    w = widgets.SelectMultiple(
        description='Lab Events',
        options=l_options
    )

    @w.on_displayed
    def on_displayed(w):
        w.selected_labels = [labitems.ix[0, 'name']]

    return w

def death_multiselect():
    d_options = OrderedDict()
    d_options['Died during ICU stay'] = 'icustay_death'
    d_options['Died during hospital admission'] = 'hadm_death'
    d_options['Died within 12 months of hospital admission'] = 'death_within_12mo'

    return widgets.RadioButtons(
        description='Death',
        options=d_options
    )

    