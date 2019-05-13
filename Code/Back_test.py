# -*- coding: utf-8 -*-
# Time: 9/2/2018 7:55 PM
# Author: Guanlin Chen
import pandas as pd
import numpy as np
from Code.Functions import transfer_kline
from Code.Signals import signal_bolling, signal_turtle_close_open_long_short_with_stop_loss
from Code.Evaluate import equity_curve_long_short
import matplotlib.pyplot as plt
 
pd.set_option('expand_frame_repr', False)  # do not wrap
pd.set_option('display.max_rows', 1000)
file_path = 'ETHUSD.h5'
# =====import data
df = pd.read_hdf(file_path, key='5T')

df = transfer_kline(df, '1H')

df = signal_turtle_close_open_long_short_with_stop_loss(df, [400, 80, 12])

df = df[df['candle_begin_time'] >= pd.to_datetime('2017-01-01')]
df.reset_index(inplace=True, drop=True)

df = equity_curve_long_short(df, leverage=1, c_rate=2.0 / 1000)
df['strats_cum_return'] = df['equity_curve']
df['ETH_cum_return'] = (df['close'].pct_change(1) + 1).cumprod()
df.set_index('candle_begin_time', inplace=True)

# calculate maximum profit and loss for single trading
df2 = df.groupby('start_time').apply(lambda x: x.iloc[-1]['equity_curve'] / x.iloc[0]['equity_curve'] - 1)
print(df2.max())
print(df2.min())

# maximum continuous profit

exit()
df['max2here'] = df['equity_curve'].expanding().max()
df['dd2here'] = df['equity_curve'] / df['max2here'] - 1
end_date, max_draw_down = tuple(df.sort_values(by=['dd2here']).iloc[0][['candle_begin_time', 'dd2here']])

print(end_date, max_draw_down)

df[['ETH_cum_return', 'strats_cum_return']].plot(title='ETHUSD ', figsize=(10, 6))
plt.show()

exit()

# ==== finding optimal parameters
n_list = range(100, 500, 50)
m_list = np.arange(0.5, 5, 0.5)

_df = pd.DataFrame()
for m in m_list:
    for n in n_list:
        para = [n, m]
        # calculate signal
        df = signal_bolling(df.copy(), para)
        df = equity_curve_long_short(df, leverage=3, c_rate=2.0 / 1000)
        print(para, 'final return', df.iloc[-1]['equity_curve'])
        _df.loc[str(para), 'final_return'] = df.iloc[-1]['equity_curve']
print(_df.sort_values(by=['final_return'], ascending=False))
