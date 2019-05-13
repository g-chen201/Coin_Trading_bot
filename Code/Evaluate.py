# -*- coding: utf-8 -*-
# Time: 9/5/2018 7:51 PM
# Author: Guanlin Chen
import pandas as pd


# df = pd.read_hdf(r'C:\Users\Jason\Desktop\Trade_Program\Data\eth_bolling_signal.h5', key='all_data')
def equity_curve_long_short(df, leverage=3, c_rate=2.0 / 1000, min_margin_rate=0.15):
    """

    :param df: includes signal and pos raw data dataframe
    :param leverage:  maximum 3 times leverage in Bitfinex，leverage_rate is in (0, 3]
    :param c_rate:  commission rate
    :param min_margin_rate:  maintenance margin，should be 15% of your borrowing
    :return:
    """

    # == Select time interval
    # df = df[df['candle_begin_time'] >= pd.to_datetime('2017')]
    # df.reset_index(drop=True, inplace=True)
    init_cash = 100
    fee = init_cash * leverage * c_rate
    min_margin = init_cash * leverage * min_margin_rate

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
    # ===construct feature columns
    df.loc[open_condition, 'start_time'] = df['candle_begin_time']
    df['start_time'].fillna(method='ffill', inplace=True)
    df.loc[df['pos'] == 0, 'start_time'] = pd.NaT

    # ===Calculate position profit, and must be grouby
    df.loc[open_condition, 'position_curve'] = (1 + df['buy_at_open_change'])

    group_num = len(df.groupby('start_time'))
    if group_num > 1:
        t = df.groupby('start_time').apply(lambda x: (x['close'] / x.iloc[0]['close']) * x.iloc[0]['position_curve'])
        # t = df.groupby('start_time').apply(lambda x: ((x['close'].pct_change() + 1).cumprod()) * x.iloc[0]['position_curve'])
        # missing some value, why?
        t = t.reset_index(level=[0])
        df['position_curve'] = t['close']
    # elif group_num == 1:
    #     t = df.groupby('start_time')[['close', 'position']].apply(
    #         lambda x: x['close'] / x.iloc[0]['close'] * x.iloc[0]['position'])
    #     df['position'] = t.T.iloc[:, 0]

    # ==for the convenience for the further step, do not consider close position here
    df['position'] = init_cash * leverage * df['position_curve']

    # == Calculate Max and min for further Forced close calculation
    df['position_min'] = df['position'] * df['low'] / df['close']
    df['position_max'] = df['position'] * df['high'] / df['close']

    # == this is true position
    df.loc[close_condition, 'position'] *= (1 + df['sell_at_next_change'])

    # == profit with each position
    df['profit'] = (df['position'] - init_cash * leverage) * df['pos']
    df.loc[df['pos'] == 1, 'profit_min'] = (df['position_min'] - init_cash * leverage) * df['pos']
    df.loc[df['pos'] == -1, 'profit_min'] = (df['position_max'] - init_cash * leverage) * df['pos']
    # df.loc[df['pos'] == 1, 'profit_min'] = df['profit'] - (df['position'] - df['position_min'])
    # df.loc[df['pos'] == -1, 'profit_min'] = df['profit'] - (df['position_max'] - df['position'])

    # == actual cash

    df['cash'] = init_cash - fee
    df['cash'] = df['cash'] + df['profit']
    df['cash_min'] = df['cash'] - (df['profit'] - df['profit_min'])
    # df['cash_min'] = init_cash - fee + df['profit_min'] 也行
    df.loc[close_condition, 'cash'] -= df['position'] * c_rate  # 减去平仓的手续费

    # == if position was forced close
    _index = df[df['cash_min'] <= min_margin].index
    if len(_index) > 0:
        print('有爆仓')
        df.loc[_index, 'forced_close_row'] = 1
        df['forced_close_row'] = df.groupby('start_time')['forced_close_row'].fillna(method='ffill')
        df.loc[(df['forced_close_row'] == 1) & (df['forced_close_row'].shift(1) != 1), 'cash_forced_close'] = df[
            'cash_min']  # tiny error here, not big deal
        # df.loc[(df['forced_close'] != df['forced_close'].shift(1)) & (df['forced_close'] is not None), 'cash_forced_close'] = df['cash_min']
        df.loc[df['forced_close_row'] == 1, 'cash'] = None
        df['cash'].fillna(value=df['cash_forced_close'], inplace=True)
        df['cash'] = df.groupby('start_time')['cash'].fillna(method='ffill')
        df.drop(['cash_forced_close', 'forced_close_row'], axis=1, inplace=True)

    # ==calculate equity curve
    df['equity_change'] = df['cash'].pct_change()
    df['equity_change'].fillna(value=0, inplace=True)
    df.loc[open_condition, 'equity_change'] = df.loc[open_condition, 'cash'] / init_cash - 1
    df['equity_curve'] = (1 + df['equity_change']).cumprod()

    # ==drop redundant data
    df.drop(['buy_at_open_change', 'sell_at_next_change', 'position_curve', 'position', 'position_max',
             'position_min', 'profit', 'profit_min', 'cash', 'cash_min'], axis=1, inplace=True)

    return df
