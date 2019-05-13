# -*- coding: utf-8 -*-
# Time: 9/2/2018 8:15 PM
# Author: Guanlin Chen

import pandas as pd

pd.set_option('expand_frame_repr', False)  # do not wrap
pd.set_option('display.max_rows', 1000)


def transfer_kline(df, rule_type='15T'):
    """
    Transfer candle_data to a specific time period
    :param df: a Data_frame that has [candle_begin_time, open, high, low, low, volume]
    :param rule_type: H: hourly, T: minutely, S: secondly, D: day, A : year
                      L: milliseconds, U: microsecond, N: nano
                     BH: business hour, SM: semi-month, B: business day, BA: business year end
    :return: transformed data_frame
    """

    df = df.resample(rule=rule_type, on='candle_begin_time', base=0, label='left', closed='left').agg(
        {'open': 'first',
         'high': 'max',
         'low': 'min',
         'close': 'last',
         'volume': 'sum'})
    # in case, some coins do not trade frequently, so has missing value
    df.dropna(subset=['open'], how='any', inplace=True)  # how default is any
    df = df[df['volume'] > 0]

    df.reset_index(inplace=True)
    df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']]

    return df
