# -*- coding: utf-8 -*-
# Time: 9/5/2018 7:51 PM
# Author: Guanlin Chen
import pandas as pd
# df = pd.read_hdf(r'C:\Users\Jason\Desktop\Trade_Program\Data\eth_bolling_signal.h5', key='all_data')
def equity_curve_long_short(df, leverage=3, c_rate=2.0/1000, min_margin_rate=0.15):
    """

    :param df:  带有signal和pos的原始数据
    :param leverage:  bfx交易所最多提供3倍杠杆，leverage_rate可以在(0, 3]区间选择
    :param c_rate:  手续费
    :param min_margin_rate:  最低保证金比例，必须占到借来资产的15%
    :return:
    """

    """
    思路
    因为是margin交易，所以最后的资金曲线并不是仓位的变化率，而是初始资金变化率
    最终目标： pct = cash(n) / cash(0) - 1
    分解： cash(n) = (cash(0) - fee) + profit
        profit = position(n) - position(0) = position(0) * pct(position)
        position(0) = cash(0) * leverage
        ==>pct = [(cash(0) - cash(0) * leverage*r) + cash(0) * leverage * pct(cash(0) * leverage)]/ cash(0) - 1
        可以看出即时考虑手续费，cash的变化，也是跟初始资金无关的，但每一次持仓算profit的position(0)基数的基数是不同的，
        所以，最简便的方法是分组计算position,每次都假设投100块，最终结果并无影响
        所以，现在就是要求pct(position)，从而算出产生的profit，然后算出cash(n)
    """
    # == 选取时间段
    # df = df[df['candle_begin_time'] >= pd.to_datetime('2017')]
    # df.reset_index(drop=True, inplace=True)
    init_cash = 100
    fee = init_cash * leverage * c_rate
    min_margin = init_cash * leverage * min_margin_rate  # 最低保证金

    df['buy_at_open_change'] = df['close'] / df['open'] - 1
    # df['close_pct_change'] = df['close'].pct_change(1)
    df['sell_at_next_change'] = df['open'].shift(-1) / df['close'] - 1
    df.loc[len(df) - 1, 'sell_at_next_change'] = 0

    condition1 = df['pos'] != 0
    condition2 = df['pos'] != df['pos'].shift(1)
    open_condition = condition1 & condition2

    condition1 = df['pos'] != 0
    condition2 = df['pos'] != df['pos'].shift(-1)
    close_condition = condition1 & condition2
    # ===构造特征列
    df.loc[open_condition, 'start_time'] = df['candle_begin_time']
    df['start_time'].fillna(method='ffill', inplace=True)
    df.loc[df['pos'] == 0, 'start_time'] = pd.NaT

    # ===计算position收益率,必须分组算，不能先总体通过累乘算仓位资金曲线，因为那样会累积一开始的收益
    df.loc[open_condition, 'position_curve'] = (1 + df['buy_at_open_change'])

    group_num = len(df.groupby('start_time'))
    if group_num > 1:
        t = df.groupby('start_time').apply(lambda x: (x['close'] / x.iloc[0]['close']) * x.iloc[0]['position_curve'])
        # t = df.groupby('start_time').apply(lambda x: ((x['close'].pct_change() + 1).cumprod()) * x.iloc[0]['position_curve'])
        # 会缺值?
        t = t.reset_index(level=[0])
        df['position_curve'] = t['close']
    # elif group_num == 1:
    #     t = df.groupby('start_time')[['close', 'position']].apply(
    #         lambda x: x['close'] / x.iloc[0]['close'] * x.iloc[0]['position'])
    #     df['position'] = t.T.iloc[:, 0]

    # ==这里先算出当时的仓位，方便后面算利润和手续费，先不考平仓时的仓位变动
    df['position'] = init_cash * leverage * df['position_curve']

    # == 算最值，方便后面计算爆仓。必须现在算，不然后期根据真实仓位算出的不对。
    # == 不考虑平仓时的变动，算出的仓位最值才是正确的
    df['position_min'] = df['position'] * df['low'] / df['close']
    df['position_max'] = df['position'] * df['high'] / df['close']

    # == 这时再计算平仓时的真实仓位
    df.loc[close_condition, 'position'] *= (1 + df['sell_at_next_change'])

    # == 计算持仓利润
    df['profit'] = (df['position'] - init_cash * leverage) * df['pos']
    df.loc[df['pos'] == 1, 'profit_min'] = (df['position_min'] - init_cash * leverage) * df['pos']
    df.loc[df['pos'] == -1, 'profit_min'] = (df['position_max'] - init_cash * leverage) * df['pos']
    # df.loc[df['pos'] == 1, 'profit_min'] = df['profit'] - (df['position'] - df['position_min'])
    # df.loc[df['pos'] == -1, 'profit_min'] = df['profit'] - (df['position_max'] - df['position'])

    # == 计算实际资金量

    df['cash'] = init_cash - fee
    df['cash'] = df['cash'] + df['profit']
    df['cash_min'] = df['cash'] - (df['profit'] - df['profit_min'])
    # df['cash_min'] = init_cash - fee + df['profit_min'] 也行
    df.loc[close_condition, 'cash'] -= df['position'] * c_rate  # 减去平仓的手续费

    # == 判断是否会爆仓
    _index = df[df['cash_min'] <= min_margin].index
    if len(_index) > 0:
        print('有爆仓')
        df.loc[_index, 'forced_close_row'] = 1
        df['forced_close_row'] = df.groupby('start_time')['forced_close_row'].fillna(method='ffill')
        df.loc[(df['forced_close_row'] == 1) & (df['forced_close_row'].shift(1) != 1), 'cash_forced_close'] = df[
            'cash_min']  # 此处是有问题的
        # df.loc[(df['forced_close'] != df['forced_close'].shift(1)) & (df['forced_close'] is not None), 'cash_forced_close'] = df['cash_min']
        df.loc[df['forced_close_row'] == 1, 'cash'] = None
        df['cash'].fillna(value=df['cash_forced_close'], inplace=True)
        df['cash'] = df.groupby('start_time')['cash'].fillna(method='ffill')
        df.drop(['cash_forced_close', 'forced_close_row'], axis=1, inplace=True)  # 删除不必要的数据

    # ==计算资金曲线
    df['equity_change'] = df['cash'].pct_change()
    df['equity_change'].fillna(value=0, inplace=True)
    df.loc[open_condition, 'equity_change'] = df.loc[open_condition, 'cash'] / init_cash - 1
    df['equity_curve'] = (1 + df['equity_change']).cumprod()

    # ==删除不必要数据
    df.drop(['buy_at_open_change', 'sell_at_next_change', 'position_curve', 'position', 'position_max',
             'position_min', 'profit', 'profit_min', 'cash', 'cash_min'], axis=1, inplace=True)

    return df
