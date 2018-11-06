# -*- coding: utf-8 -*-
# Time: 11/5/2018 5:18 PM
# Author: Guanlin Chen
import pandas as pd
import numpy as np
from Code.Functions import transfer_kline
from statsmodels.tsa.stattools import adfuller, coint
from matplotlib import pyplot as plt
pd.set_option('display.max_rows', 100)

df_eth = pd.read_hdf(r'C:\Users\Jason\Desktop\coin\Data\eth_1min_data.h5')
df_eos = pd.read_hdf(r'C:\Users\Jason\Desktop\coin\Data\eos_1min_perfect.h5')
df_eth = transfer_kline(df_eth, rule_type='30T')
df_eos = transfer_kline(df_eos, rule_type='30T')


def choose_period(df, start_time='2017-07-02', end_time='2018-03-21'):
    df = df[(df['candle_begin_time'] >= start_time) & (df['candle_begin_time'] <= end_time)]
    df.set_index('candle_begin_time', inplace=True)
    return df


df_eth = choose_period(df_eth)
df_eos = choose_period(df_eos)
all_data = pd.DataFrame({'eth': df_eth['close'], 'eos': df_eos['close']})
all_data.columns = ['eth_close', 'eos_close']
all_data['eth_close'].plot(figsize=(8,6))
plt.ylabel('sss')
plt.show()