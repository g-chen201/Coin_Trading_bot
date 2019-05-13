# -*- coding: utf-8 -*-
# Time: 9/5/2018 11:24 PM
# Author: Guanlin Chen
from datetime import datetime, timedelta
import pandas as pd
from time import sleep
import ccxt
from Code.Trade import next_run_time, place_order, get_bitfinex_candle_data, auto_send_email, fetch_position, send_dingding_msg, fetch_margin_balance
from Code.Signals import signal_bolling

pd.set_option('display.max_rows', 1000)
pd.set_option('expand_frame_repr', False)  # do not wrap

# ===== Parameters
time_interval = '30m'  # run time interval
exchange = ccxt.bitfinex()
exchange2 = ccxt.bitfinex2()  # bitfinex has two different API
exchange.apiKey = 'your key'
exchange.secret = 'your secret'
exchange2.apiKey = 'your key'
exchange2.secret = 'your secret'

symbol = 'EOS/USDT'  # should comply with exchange
symbol_for_position = 'eosusd'
base_coin = symbol.split('/')[-1]
trade_coin = symbol.split('/')[0]
leverage = 2
para = [300, 3.4]  # strats parameter
email = 'your email'
# =====主程序
while True:
    # ===monitoring email content
    email_title = 'Strats Report'
    email_content = 'eos'

    # === update balance info from server
    # balance = exchange.fetch_balance()['total']
    margin_balance = fetch_margin_balance(exchange, symbol)
    position = fetch_position(exchange, symbol_for_position)
    # base_coin_amount = float(balance[base_coin])
    # trade_coin_amount = float(balance[trade_coin])
    # print('current asset:\n', base_coin, base_coin_amount, trade_coin, trade_coin_amount)
    # exit()
    # # ===sleep until next run time
    run_time = next_run_time(time_interval)
    sleep(max(0, (run_time - datetime.now()).seconds))
    while True:  # when it's close to the target time, run
        if datetime.now() < run_time:
            continue
        else:
            break

    # ===check the data if it is newest
    while True:
        # fetch data
        df = get_bitfinex_candle_data(exchange, symbol, time_interval)
        # check if the data is newest
        _temp = df[df['candle_begin_time_GMT8'] == (run_time - timedelta(minutes=int(time_interval.strip('m'))))]
        if _temp.empty:
            print('did not include the newest data，fetching again')
            sleep(5)
            continue
        else:
            break

    # === producing trading signal
    df = df[df['candle_begin_time_GMT8'] < pd.to_datetime(run_time)]  # deleted target_time data
    df = signal_bolling(df, para=para)
    # print(df)
    # exit()
    signal = df.iloc[-1]['signal']
    # signal = -1  # for testing
    print('\ntrading signal', signal)

    # =====No position and sell short
    if margin_balance > 0 and signal == -1 and position == 0:
        print('\nsell')
        # get the most recent sell price
        price = exchange.fetch_ticker(symbol)['bid']
        sell_amount = (margin_balance * leverage) / price
        # placing order
        place_order(exchange, order_type='limit', buy_or_sell='sell', symbol=symbol, price=price * 0.98,
                    amount=sell_amount)
        email_title += '_sell_' + trade_coin
        email_content += 'sell_info：\n'
        email_content += 'sell_amount：' + str(sell_amount) + '\n'
        email_content += 'sell_price：' + str(price) + '\n'
        auto_send_email(email, 'Sell short', email_content)

    # =====No position and buy
    if margin_balance > 0 and signal == 1 and position == 0:
        print('\nbuy')
        # get the most recent buy price
        price = exchange.fetch_ticker(symbol)['ask']
        buy_amount = leverage * margin_balance / price
        # get the most recent sell_price
        place_order(exchange, order_type='limit', buy_or_sell='buy', symbol=symbol, price=price * 1.02,
                    amount=buy_amount)
        email_title += '_buy_' + trade_coin
        email_content += 'buy_info：\n'
        email_content += 'buy_amount：' + str(buy_amount) + '\n'
        email_content += 'buy_price：' + str(price) + '\n'
        auto_send_email(email, 'Buy long', email_content)
    # =====close short and long buy
    if signal == 1 and position < 0:
        print('\nbuy')
        # get the most recent buy price
        price = exchange.fetch_ticker(symbol)['ask']
        buy_amount1 = position * -1
        buy_amount2 = leverage * margin_balance / price
        place_order(exchange, order_type='limit', buy_or_sell='buy', symbol=symbol, price=price * 1.02,
                    amount=buy_amount1)
        place_order(exchange, order_type='limit', buy_or_sell='buy', symbol=symbol, price=price * 1.02,
                    amount=buy_amount2)

        email_title += '_buy_' + trade_coin
        email_content += 'buy_info：\n'
        email_content += 'buy_amount：' + str(buy_amount1) + '\n'
        email_content += 'buy_price：' + str(price) + 'buy and close pos' + '\n'
        email_content += 'buy_amount：' + str(buy_amount2) + 'reopen position' + ' ' + '\n'
        email_content += 'buy_price：' + str(price) + '\n'
        auto_send_email(email, 'close short and long buy', email_content)

    # =====close long and sell short
    if signal == -1 and position > 0:
        print('\nsell')
        price = exchange.fetch_ticker(symbol)['bid']
        sell_amount1 = position
        sell_amount2 = (margin_balance * leverage) / price

        place_order(exchange, order_type='limit', buy_or_sell='sell', symbol=symbol, price=price * 0.98,
                    amount=sell_amount1)
        place_order(exchange, order_type='limit', buy_or_sell='sell', symbol=symbol, price=price * 0.98,
                    amount=sell_amount2)

        email_title += '_sell_' + trade_coin
        email_content += 'sell_info：\n'
        email_content += 'sell_amount：' + str(sell_amount1) + 'sell and close pos' + ' ' + '\n'
        email_content += 'sell_price：' + str(price) + '\n'
        email_content += 'sell_amount：' + str(sell_amount2) + 'reopen pos' + ' ' + '\n'
        email_content += 'sell_price：' + str(price) + '\n'
        auto_send_email(email, 'close long and sell short', email_content)
    # close short position
    if signal == 0 and position < 0:
        print('\nbuy')
        # get the most recent buy price
        price = exchange.fetch_ticker(symbol)['ask']
        buy_amount = position * -1
        # get the most recent sell price
        place_order(exchange, order_type='limit', buy_or_sell='buy', symbol=symbol, price=price * 1.02,
                    amount=buy_amount)

        email_title += '_buy_' + trade_coin
        email_content += 'buy_info：\n'
        email_content += 'buy_amount：' + str(buy_amount) + 'buy and close pos' + '\n'
        email_content += 'buy_price：' + str(price) + '\n'
        auto_send_email(email, 'close short position', email_content)
    # close long position
    if signal == 0 and position > 0:
        print('\nsell')
        # get the most recent sell price
        price = exchange.fetch_ticker(symbol)['bid']
        sell_amount = position
        place_order(exchange, order_type='limit', buy_or_sell='sell', symbol=symbol, price=price * 0.98,
                    amount=sell_amount)

        email_title += '_sell_' + trade_coin
        email_content += 'sell_info：\n'
        email_content += 'sell_amount：' + str(sell_amount) + 'sell and close pos' + '\n'
        email_content += 'sell_price：' + str(price) + '\n'
        auto_send_email(email, 'close long position', email_content)

    # =====sending email/dingding
    # sending the report to dingding every 30 min
    if run_time.minute % 30 == 0:
        send_dingding_msg(email_content)

    # =====trading close
    print(email_title)
    print(email_content)
    print('=====Finished this time running \n')
    sleep(60 * 10)
