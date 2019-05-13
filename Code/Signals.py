# -*- coding: utf-8 -*-
# Time: 9/3/2018 12:33 AM
# Author: Guanlin Chen

# ========= This is a sample strat program, and it only contains a simple bollinger strat

def signal_bolling(df, para=[100, 2]):
    n = para[0]
    m = para[1]

    df['median'] = df['close'].rolling(n, min_periods=1).mean()  # min_period=1: calculate from first period
    df['std'] = df['close'].rolling(n, min_periods=1).std(ddof=0)  # ddof is degree of freedom, Bitfinex is zero

    df['upper'] = df['median'] + m * df['std']
    df['lower'] = df['median'] - m * df['std']

    # ===open long signal
    condition1 = df['close'] > df['upper']
    condition2 = df['close'].shift(1) <= df['upper'].shift(1)
    df.loc[condition1 & condition2, 'signal_long'] = 1

    # ===close long signal
    condition1 = df['close'] < df['median']
    condition2 = df['close'].shift(1) >= df['median'].shift(1)  #
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

    temp = df[df['signal'].notnull()][['signal']]
    temp = temp[temp['signal'] != temp['signal'].shift(1)]
    df['signal'] = temp['signal']

    df.drop(['median', 'std', 'upper', 'lower', 'signal_long', 'signal_short'], axis=1, inplace=True)

    # calculate position
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
