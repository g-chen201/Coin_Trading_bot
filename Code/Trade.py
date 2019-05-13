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

        # sleep直到靠近目标时间之前
        if (target_time - datetime.now()).seconds < ahead_time+1:
            print('距离target_time不足', ahead_time, '秒，下下个周期再运行')
            target_time += timedelta(minutes=time_interval)
        print('下次运行时间', target_time)
        return target_time
    else:
        exit('time_interval doesn\'t end with m')


# 获取okex的k线数据
def get_okex_candle_data(exchange, symbol, time_interval):

    # 抓取数据
    content = exchange.fetch_ohlcv(symbol, timeframe=time_interval, since=0) # since=0 是okex特有， 其他用since

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
    # 计算从什么时候开始获取K线，但保证获取的k线数等于lines
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
            print('下单报错，1s后重试', e)
            time.sleep(1)
            try_num += 1
    auto_send_email('jasonchen701@hotmail.com', '程序出错', 'candle失败')
    exit()


# 下单
def place_order(exchange, order_type, buy_or_sell, symbol, price, amount):
    """
    下单
    :param exchange: 交易所
    :param order_type: limit, market
    :param buy_or_sell: buy, sell
    :param symbol: 买卖品种
    :param price: 当market订单的时候，price无效
    :param amount: 买卖量
    :return:
    """
    for i in range(5):
        try:
            # 限价单
            if order_type == 'limit':
                # 买
                if buy_or_sell == 'buy':
                    order_info = exchange.create_limit_buy_order(symbol, amount, price, {'type': 'limit'})  # 买单
                # 卖
                elif buy_or_sell == 'sell':
                    order_info = exchange.create_limit_sell_order(symbol, amount, price, {'type': 'limit'})  # 卖单
            # 市价单
            elif order_type == 'market':
                # 买
                if buy_or_sell == 'buy':
                    order_info = exchange.create_market_buy_order(symbol=symbol, amount=amount)  # 买单
                # 卖
                elif buy_or_sell == 'sell':
                    order_info = exchange.create_market_sell_order(symbol=symbol, amount=amount)  # 卖单
            else:
                pass

            print('下单成功：', order_type, buy_or_sell, symbol, price, amount)
            print('下单信息：', order_info, '\n')
            return order_info

        except Exception as e:
            print('下单报错，1s后重试', e)
            time.sleep(1)

    print('下单报错次数过多，程序终止')
    auto_send_email('jasonchen701@hotmail.com', '程序出错', '下单失败')
    exit()

def fetch_margin_balance(exchange, symbol):
    for i in range(5):
        try:
            margin_balance = exchange.fetch_balance({'type': 'trading'})['USDT']['free']
            return margin_balance

        except Exception as e:
            print('获取balance出错，2s后重试', e)
            time.sleep(2)

    print('查询次数数过多，程序终止')
    auto_send_email('jasonchen701@hotmail.com', '程序出错', '获取balance失败')
    exit()


# automated sending email
def auto_send_email(to_address, subject, content, from_address='iverson201@qq.com', if_add_time=True):
    """
    :param to_address:
    :param subject:
    :param content:
    :param from_address:
    :return:
    使用qq mail发送邮件的程序
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

        print('邮件发送成功')
    except Exception as err:
        print('邮件发送失败', err)


def send_dingding_msg(content, robot_id='25e8899c5426f4fe98075b0b37c9022c4a1de87fe5ca13b57dbf37592dfbeb74'):

    try:
        msg = {
            "msgtype": "text",
            "text": {"content": content + '\n' + datetime.now().strftime("%m-%d %H:%M:%S")}}

        Headers = {"Content-Type": "application/json ;charset=utf-8 "}
        url = 'https://oapi.dingtalk.com/robot/send?access_token=' + robot_id
        body = json.dumps(msg)
        requests.post(url, data=body, headers=Headers)
    except Exception as err:
        print('钉钉发送失败', err)

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
            print('获取仓位出错，2s后重试', e)
            time.sleep(2)

    print('Too much querying, the program stopped.')
    auto_send_email('jasonchen701@hotmail.com', '程序出错', '获取仓位失败')
    exit()