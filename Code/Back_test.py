# -*- coding: utf-8 -*-
# Time: 9/2/2018 7:55 PM
# Author: Guanlin Chen
import pandas as pd
import numpy as np
from Code.Functions import transfer_kline
from Code.Signals import signal_bolling
from Code.Evaluate import equity_curve_long_short

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 1000)

# =====导入数据
df = pd.read_hdf(r'C:\Users\Jason\Desktop\Trade_Program\Data\eth_bolling_signal.h5', key='all_data')

df = transfer_kline(df, '15T')

df = signal_bolling(df, [150, 3])

# === 选取时间范围
df = df[df['candle_begin_time'] >= pd.to_datetime('2017-01-01')]
df.reset_index(inplace=True, drop=True)



# ==== 寻找最优参数
n_list = range(100, 500, 50)
m_list = np.arange(0.5, 5, 0.5)

_df = pd.DataFrame()
for m in m_list:
    for n in n_list:
        para = [n, m]
        # 计算信号
        df = signal_bolling(df.copy(), para)
        df = equity_curve_long_short(df, leverage=3, c_rate= 2.0 / 1000)
        print(para, 'final return', df.iloc[-1]['equity_curve'])
        _df.loc[str(para), 'final_return'] = df.iloc[-1]['equity_curve']
print(_df.sort_values(by=['final_return'], ascending=False))
