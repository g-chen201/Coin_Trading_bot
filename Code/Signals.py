# -*- coding: utf-8 -*-
# Time: 9/3/2018 12:33 AM
# Author: Guanlin Chen

# =========bolling

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

def signal_moving_average(df, para=[5, 60]):
    """
    简单的移动平均线策略
    当短期均线由下向上穿过长期均线的时候，买入；然后由上向下穿过的时候，卖出。
    :param df:  原始数据
    :param para:  参数，[ma_short, ma_long]
    :return:
    """

    # ===计算指标
    ma_short = para[0]
    ma_long = para[1]

    # 计算均线
    df['ma_short'] = df['close'].rolling(ma_short, min_periods=1).mean()
    df['ma_long'] = df['close'].rolling(ma_long, min_periods=1).mean()

    # ===找出买入信号
    condition1 = df['ma_short'] > df['ma_long']  # 短期均线 > 长期均线
    condition2 = df['ma_short'].shift(1) <= df['ma_long'].shift(1)  # 之前的短期均线 <= 长期均线
    df.loc[condition1 & condition2, 'signal'] = 1  # 将产生做多信号的那根K线的signal设置为1，1代表做多

    # ===找出卖出信号
    condition1 = df['ma_short'] < df['ma_long']  # 短期均线 < 长期均线
    condition2 = df['ma_short'].shift(1) >= df['ma_long'].shift(1)  # 之前的短期均线 >= 长期均线
    df.loc[condition1 & condition2, 'signal'] = 0  # 将产生平仓信号当天的signal设置为0，0代表平仓

    df.drop(['ma_short', 'ma_long'], axis=1, inplace=True)

    # ===由signal计算出实际的每天持有仓位
    # signal的计算运用了收盘价，是每根K线收盘之后产生的信号，到第二根开盘的时候才买入，仓位才会改变。
    df['pos'] = df['signal'].shift()
    df['pos'].fillna(method='ffill', inplace=True)
    df['pos'].fillna(value=0, inplace=True)  # 将初始行数的position补全为0

    return df
