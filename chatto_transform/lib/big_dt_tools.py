import datetime

def big_dt_to_num(big_dt_col):
    return big_dt_col.map(datetime.datetime.timestamp, na_action='ignore')

def num_to_big_dt(num_col):
    return num_col.map(datetime.datetime.fromtimestamp, na_action='ignore')