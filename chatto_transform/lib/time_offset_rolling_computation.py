import pandas as pd
from datetime import datetime, timedelta
import numpy as np

import operator

def time_offset_rolling_computation(data_df_ser, window_i_s, min_periods_i=1, center_b=False, op=None):
    """ Function that computes a rolling mean

    Credit goes to user2689410 at http://stackoverflow.com/questions/15771472/pandas-rolling-mean-by-time-interval

    Parameters
    ----------
    data_df_ser : DataFrame or Series
         If a DataFrame is passed, the time_offset_rolling_mean_df_ser is computed for all columns.
    window_i_s : int or string
         If int is passed, window_i_s is the number of observations used for calculating
         the statistic, as defined by the function pd.time_offset_rolling_mean_df_ser()
         If a string is passed, it must be a frequency string, e.g. '90S'. This is
         internally converted into a DateOffset object, representing the window_i_s size.
    min_periods_i : int
         Minimum number of observations in window_i_s required to have a value.

    Returns
    -------
    Series or DataFrame, if more than one column

    >>> idx = [
    ...     datetime(2011, 2, 7, 0, 0),
    ...     datetime(2011, 2, 7, 0, 1),
    ...     datetime(2011, 2, 7, 0, 1, 30),
    ...     datetime(2011, 2, 7, 0, 2),
    ...     datetime(2011, 2, 7, 0, 4),
    ...     datetime(2011, 2, 7, 0, 5),
    ...     datetime(2011, 2, 7, 0, 5, 10),
    ...     datetime(2011, 2, 7, 0, 6),
    ...     datetime(2011, 2, 7, 0, 8),
    ...     datetime(2011, 2, 7, 0, 9)]
    >>> idx = pd.Index(idx)
    >>> vals = np.arange(len(idx)).astype(float)
    >>> ser = pd.Series(vals, index=idx)
    >>> df = pd.DataFrame({'s1':ser, 's2':ser+1})
    >>> time_offset_rolling_mean_df_ser(df, window_i_s='2min')
                          s1   s2
    2011-02-07 00:00:00  0.0  1.0
    2011-02-07 00:01:00  0.5  1.5
    2011-02-07 00:01:30  1.0  2.0
    2011-02-07 00:02:00  2.0  3.0
    2011-02-07 00:04:00  4.0  5.0
    2011-02-07 00:05:00  4.5  5.5
    2011-02-07 00:05:10  5.0  6.0
    2011-02-07 00:06:00  6.0  7.0
    2011-02-07 00:08:00  8.0  9.0
    2011-02-07 00:09:00  8.5  9.5
    """
    def calculate_at_ts(ts, f, offset=None):
        """Function (closure) to apply that actually computes the rolling mean"""
        if offset is None:
            offset = pd.datetools.to_offset(window_i_s)
        if center_b == False:
            dslice_df_ser = data_df_ser[
                ts-offset+timedelta(0,0,1):
                ts
            ]
            # adding a microsecond because when slicing with labels start and endpoint
            # are inclusive
        else:
            dslice_df_ser = data_df_ser[
                ts-offset/2:+timedelta(0,0,1):
                ts+offset/2
            ]

        result = f(dslice_df_ser)

        if  (isinstance(dslice_df_ser, pd.DataFrame) and dslice_df_ser.shape[0] < min_periods_i) or \
            (isinstance(dslice_df_ser, pd.Series) and dslice_df_ser.size < min_periods_i):
            result = result * np.nan # keeps number format and whether Series or DataFrame
        
        return result

    #if isinstance(window_i_s, int):
    #    mean_df_ser = pd.rolling_mean(data_df_ser, window=window_i_s, min_periods=min_periods_i, center=center_b)
    #elif isinstance(window_i_s, str):
    offset = pd.datetools.to_offset(window_i_s)
    idx_ser = pd.Series(data_df_ser.index.to_pydatetime(), index=data_df_ser.index)
    f = None
    if op == 'mean':
        f = operator.methodcaller('mean')
    elif op == 'sum':
        f = operator.methodcaller('sum')
    mean_df_ser = idx_ser.apply(calculate_at_ts, f=f, offset=offset)

    return mean_df_ser