# -*- coding: utf-8 -*-
# Time: 9/5/2018 11:22 PM
# Author: Guanlin Chen
from datetime import datetime, timedelta
import time
import pandas as pd
from email.mime.text import MIMEText
from smtplib import SMTP
import requests
import json


# sleep
def next_run_time(time_interval, ahead_time=1):
    if time_interval.endswith('m'):
        now_time = datetime.now()
        time_interval = int(time_interval.strip('m'))

        target_min = (int(now_time.minute / time_interval) + 1) * time_interval
        if target_min < 60:
            target_time = now_time.replace(minute=target_min, second=0, microsecond=0)
        else:
            if now_time.hour == 23:
                target_time = now_time.replace(hour=0, minute=0, second=0, microsecond=0)
                target_time += timedelta(days=1)
            else:
                target_time = now_time.replace(hour=now_time.hour + 1, minute=0, second=0, microsecond=0)

        # sleep until next run time
        if (target_time - datetime.now()).seconds < ahead_time + 1:
            print('Less than', ahead_time, 'Sec，program will run in next period')
            target_time += timedelta(minutes=time_interval)
        print('Next Run Time', target_time)
        return target_time
    else:
        exit('time_interval doesn\'t end with m')


# OKex data
def get_okex_candle_data(exchange, symbol, time_interval):
    # fetch data
    content = exchange.fetch_ohlcv(symbol, timeframe=time_interval, since=0)  # since=0 only for OKex

    #
    df = pd.DataFrame(content, dtype=float)
    df.rename(columns={0: 'MTS', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'}, inplace=True)
    df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')
    df['candle_begin_time_GMT8'] = df['candle_begin_time'] + timedelta(hours=-4)
    df = df[['candle_begin_time_GMT8', 'open', 'high', 'low', 'close', 'volume']]

    return df


def get_bitfinex_candle_data(exchange, symbol, time_interval, lines=1000):
    # scraping data

    now_time = exchange.milliseconds()
    if time_interval.endswith('m'):
        start_time = now_time - int(time_interval[:-1]) * lines * 60000
    if time_interval.endswith('h'):
        start_time = now_time - int(time_interval[:-1]) * lines * 60000 * 60
    if time_interval.endswith('d'):
        start_time = now_time - int(time_interval[:-1]) * lines * 60000 * 60 * 24
    for i in range(5):
        try_num = 0

        try:
            content = exchange.fetch_ohlcv(symbol, timeframe=time_interval, since=start_time, limit=lines)

            # organizing data
            df = pd.DataFrame(content, dtype=float)
            df.rename(columns={0: 'MTS', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'}, inplace=True)
            df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')
            df['candle_begin_time_GMT8'] = df['candle_begin_time'] + timedelta(hours=-4)
            df = df[['candle_begin_time_GMT8', 'open', 'high', 'low', 'close', 'volume']]
            return df
        except Exception as e:
            print('Placing order error，retry in 1 sec', e)
            time.sleep(1)
            try_num += 1
    auto_send_email('jasonchen701@hotmail.com', 'Error', 'fetch candle failed')
    exit()


# Place order
def place_order(exchange, order_type, buy_or_sell, symbol, price, amount):
    """
    Place order
    :param exchange: Exchange name
    :param order_type: limit, market
    :param buy_or_sell: buy, sell
    :param symbol: eg. EOS/USD
    :param price: when you place market order，this parameter is invalid
    :param amount: how much you'd like to trade
    :return:
    """
    for i in range(5):
        try:
            # limit order
            if order_type == 'limit':
                # buy order
                if buy_or_sell == 'buy':
                    order_info = exchange.create_limit_buy_order(symbol, amount, price, {'type': 'limit'})
                    # sell order
                elif buy_or_sell == 'sell':
                    order_info = exchange.create_limit_sell_order(symbol, amount, price, {'type': 'limit'})
            # market order
            elif order_type == 'market':
                if buy_or_sell == 'buy':
                    order_info = exchange.create_market_buy_order(symbol=symbol, amount=amount)
                elif buy_or_sell == 'sell':
                    order_info = exchange.create_market_sell_order(symbol=symbol, amount=amount)
            else:
                pass

            print('Placing Order successfully：', order_type, buy_or_sell, symbol, price, amount)
            print('Order Info：', order_info, '\n')
            return order_info

        except Exception as e:
            print('Placing Oder failed，retry in 1 sec', e)
            time.sleep(1)

    print('Too many times placing order，stop program')
    auto_send_email('jasonchen701@hotmail.com', 'Error', 'Placing Oder failed')
    exit()


def fetch_margin_balance(exchange, symbol):
    for i in range(5):
        try:
            margin_balance = exchange.fetch_balance({'type': 'trading'})['USDT']['free']
            return margin_balance

        except Exception as e:
            print('Fetch balance error，retry in 2 sec', e)
            time.sleep(2)

    print('Too many requesting，Stop program')
    auto_send_email('jasonchen701@hotmail.com', 'Error', 'fetch balance fail')
    exit()


# automated sending email
def auto_send_email(to_address, subject, content, from_address='iverson201@qq.com', if_add_time=True):
    """
    :param to_address:
    :param subject:
    :param content:
    :param from_address:
    :return:
    Used qq mail send email
    """
    try:
        if if_add_time:
            msg = MIMEText(datetime.now().strftime("%m-%d %H:%M:%S") + '\n\n' + content)
        else:
            msg = MIMEText(content)
        msg["Subject"] = subject + ' ' + datetime.now().strftime("%m-%d %H:%M:%S")
        msg["From"] = from_address
        msg["To"] = to_address

        username = from_address
        password = 'your password'

        server = SMTP('smtp.qq.com', port=587)
        server.starttls()
        server.login(username, password)
        server.sendmail(from_address, to_address, msg.as_string())
        server.quit()

        print('Email sent successfully ')
    except Exception as err:
        print('Email sending failed', err)


def send_dingding_msg(content, robot_id='your dingding robot ID'):
    try:
        msg = {
            "msgtype": "text",
            "text": {"content": content + '\n' + datetime.now().strftime("%m-%d %H:%M:%S")}}

        Headers = {"Content-Type": "application/json ;charset=utf-8 "}
        url = 'https://oapi.dingtalk.com/robot/send?access_token=' + robot_id
        body = json.dumps(msg)
        requests.post(url, data=body, headers=Headers)
    except Exception as err:
        print('fail to send dingding', err)


def fetch_position(exchange, symbol):
    position = 0
    for i in range(5):
        try:
            position_info = exchange.private_post_positions()

            for info in position_info:
                if info['symbol'] == symbol:
                    position = float(info['amount'])
            return position

        except Exception as e:
            print('error in fetching position，retry in 2 sec', e)
            time.sleep(2)

    print('Too much querying, the program stopped.')
    auto_send_email('jasonchen701@hotmail.com', 'Error', 'fail to fetch position')
    exit()
