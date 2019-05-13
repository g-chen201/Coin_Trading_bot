# -*- coding: utf-8 -*-
# Time: 9/3/2018 12:33 AM
# Author: Guanlin Chen

# =========

def signal_bolling(df, para=[100, 2]):
    n = para[0]
    m = para[1]

    df['median'] = df['close'].rolling(n, min_periods=1).mean()  # min_period=1 表示不足100天的，有多少天算多少天的均值
    df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)  # ddof is degree of freedom, 官网是用0

    df['upper'] = df['median'] + m * df['std']
    df['lower'] = df['median'] - m * df['std']

    # ===open long signal
    condition1 = df['close'] > df['upper']
    condition2 = df['close'].shift(1) <= df['upper'].shift(1)
    df.loc[condition1 & condition2, 'signal_long'] = 1

    # ===close long signal
    condition1 = df['close'] < df['median']
    condition2 = df['close'].shift(1) >= df['median'].shift(1)  # 之前K线的收盘价 >= 中轨
    df.loc[condition1 & condition2, 'signal_long'] = 0

    # ===open short signal
    condition1 = df['close'] < df['lower']
    condition2 = df['close'].shift(1) >= df['lower'].shift(1)
    df.loc[condition1 & condition2, 'signal_short'] = -1

    # === close short signal
    condition1 = df['close'] > df['median']
    condition2 = df['close'].shift(1) <= df['median'].shift(1)
    df.loc[condition1 & condition2, 'signal_short'] = 0

    # === sum two signal to get the final signal
    df['signal'] = df[['signal_long', 'signal_short']].sum(axis=1, skipna=True, min_count=1)
    # 这里min_count必须加，不然na+ na =0, 意思是最小的非NA数量等于1，即小于1就为空

    # 这时可能还有重复信号，比如(1,nnn, 1, 1, 0, -1, -1)
    # 把第一次出现这些信号的找出来，在两个不同信号之间的，都为空
    # 1.先找出有信号的 2.在其中找出本行信号，不等于上一行信号的号 3.赋值给原来的列
    # 可能产生的问题1. [na,0, 1] 在没建仓之前出现平仓信号, 实盘下单程序中会先判断仓位，回测是根据pos判断，会补全前几行
    #              2. [1,-1] 回测没问题，实盘需要再加平仓判断

    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]  # 只需要本行不等于上一行一个条件就行了
    df['signal'] = temp['signal']
    # 这种按index赋值，时根据左边的index依次去寻找右边的相应所以，如果右边没有对应index，不管原来是什么，直接赋为NaN

    df.drop(['median', 'std', 'upper', 'lower', 'signal_long', 'signal_short'], axis=1, inplace=True)

    # 计算仓位情况
    df['pos'] = df['signal'].shift(1)
    df['pos'].fillna(method='ffill', inplace=True)
    df['pos'].fillna(value=0, inplace=True)

    return df


def signal_bolling_with_stop_loss(df, para=[100, 2]):

    pass


def signal_turtle_close_open_long_short_with_stop_loss():

    pass
def pair_trading():
    pass

def CMO():
    pass
