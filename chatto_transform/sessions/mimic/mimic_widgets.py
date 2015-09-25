import pandas as pd

import ipywidgets as widgets
import traitlets

from collections import OrderedDict
import os.path
import html

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


######################
# Comparison filters #
######################

def multi_text_match():
    t = widgets.Text()
    submitted = []
    s_box = widgets.VBox()

    def update_s_box():
        s_box.children = tuple(map(list_entry, submitted))

    @t.on_submit
    def on_submit(sender):
        if not t.value:
            return
        submitted.append(t.value)
        update_s_box()
        t.value = ''

    def list_entry(value):
        e = widgets.HTML(
            html.escape(value),
            padding='5px',
            background_color='gray',
            color='white')
        rm_button = widgets.Button(
            description='✖',
            margin='5px',
            padding='1px')
        traitlets.link((t, 'disabled'), (rm_button, 'disabled'))
        le = widgets.HBox(children=[e, rm_button])

        @rm_button.on_click
        def remove(b):
            submitted.remove(value)
            update_s_box()
        return le

    root = widgets.VBox(children=[s_box, t])
    root.add_traits(
        value=traitlets.Any(),
        disabled=traitlets.Any(),
        filter_type=traitlets.Any())
    root.value = submitted
    root.disabled = False
    root.filter_type = 'text'
    traitlets.link((root, 'disabled'), (t, 'disabled'))
    return root

def cat_select(categories):
    s = widgets.SelectMultiple(options=categories)
    s.add_traits(filter_type=traitlets.Any())
    s.filter_type = 'cat'
    return s

def num_eq_filter():
    f = widgets.FloatText()
    f.add_traits(filter_type=traitlets.Any())
    f.filter_type = 'num_eq'
    return f

def num_range_filter():
    c_min = widgets.Checkbox(description='Min:', value=False)
    f_min = widgets.FloatText()
    l_min = traitlets.link((c_min, 'value'), (f_min, 'visible'))
    min_box = widgets.HBox(children=[c_min, f_min])

    c_max = widgets.Checkbox(description='Max:', value=False)
    f_max = widgets.FloatText()
    l_max = traitlets.link((c_max, 'value'), (f_max, 'visible'))
    max_box = widgets.HBox(children=[c_max, f_max])

    def min_change(name, value):
        if f_max.value < value:
            f_max.value = value
    f_min.on_trait_change(min_change, 'value')
            
    def max_change(name, value):
        if f_min.value > value:
            f_min.value = value
    f_max.on_trait_change(max_change, 'value')
            
    root = widgets.VBox(children=[min_box, max_box])
    root.add_traits(
        min_enabled=traitlets.Any(),
        min_value=traitlets.Any(),
        max_enabled=traitlets.Any(),
        max_value=traitlets.Any(),
        filter_type=traitlets.Any()
    )
    root.filter_type = 'num_range'

    traitlets.link((c_min, 'value'), (root, 'min_enabled'))
    traitlets.link((f_min, 'value'), (root, 'min_value'))

    traitlets.link((c_max, 'value'), (root, 'max_enabled'))
    traitlets.link((f_max, 'value'), (root, 'max_value'))

    return root

    