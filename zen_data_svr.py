
# -*- coding:utf-8 -*-
#! python3

from pandas import DataFrame, Series
import pandas as pd
import numpy as np

import time
import sys
import os
import mysql.connector

from zd_logging import g_logger
from zen_kline_data import *
from utils import *
from jqdatasdk import *

from concurrent import futures
import time
import random
import grpc
import zen_data_svr_pb2
import zen_data_svr_pb2_grpc

#读取数据
g_zen_kline_data = ZenKlineData('futures', 'config.ini')

# 实现 proto 文件中定义的 ZendataHandleServicer
class ZendataHandle(zen_data_svr_pb2_grpc.ZendataHandleServicer):
    # 实现 proto 文件中定义的 rpc 调用
    def AskKlineData(self, request, context):
        g_logger.info('AskKlineData, code=%s, period=%s, from_time=%s(%d), to_ts=%s(%d)', request.code, request.period,
                      datetime.datetime.fromtimestamp(request.from_ts).strftime('%Y-%m-%d %H:%M:%S'), request.from_ts,
                      datetime.datetime.fromtimestamp(request.to_ts).strftime('%Y-%m-%d %H:%M:%S'), request.to_ts)
        klines = g_zen_kline_data.GetKlines(request.code, request.period, request.from_ts, request.to_ts)
        rsp_klines = []
        try:
            if klines is not None:
                for idx in klines.index:
                    rsp_kline = zen_data_svr_pb2.Kline(ts = klines.loc[idx, "Ts"], open = klines.loc[idx, "Open"],
                                                       high = klines.loc[idx, "High"], low = klines.loc[idx, "Low"],
                                                       close = klines.loc[idx, "Close"], vol = int(klines.loc[idx, "Volume"]),
                                                       amount = klines.loc[idx, "Amount"], is_end = 0)
                    rsp_klines.append(rsp_kline)
        except Exception as e:
            g_logger.warning(str(e))
            g_logger.exception(e)

        g_logger.info('AskKlineRsp klines length:%d', len(rsp_klines))
        return zen_data_svr_pb2.AskKlineRsp(status = 0, message="success", code=request.code,
                                            period=request.period, sec_type=request.sec_type, klines=rsp_klines)

class ZenDataSvr(threading.Thread):
    def __init__(self, threadname):
        threading.Thread.__init__(self, name='ZenDataSvr线程' + threadname)
        self.threadname = threadname

    def run(self):
        # 启动 rpc 服务
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        zen_data_svr_pb2_grpc.add_ZendataHandleServicer_to_server(ZendataHandle(), server)
        server.add_insecure_port('[::]:18101')
        server.start()
        try:
            while True:
                time.sleep(60*60*24) # 1 day in seconds
        except Exception as e:
            g_logger.warning(str(e))
            g_logger.exception(e)
            server.stop(0)

class ZenDataSync(threading.Thread):
    def __init__(self, threadname):
        threading.Thread.__init__(self, name='ZenDataSync线程' + threadname)
        self.threadname = threadname

    def run(self):
        auth('1588963xxxx', 'ABCDefxxxx')

        is_auth_succ = is_auth()
        if is_auth_succ == False:
            g_logger.warning("auth failed!")
            return -1
        else:
            g_logger.info('auth success!')

        codes = list()
        for code in g_zen_kline_data.sec_insert_ts_idx.keys():
            codes.append(code)
        if len(codes)==0:
            return -2

        while True:
            try:
                g_zen_kline_data.SyncKlines()
                time.sleep(10) # 1 day in seconds
            except Exception as e:
                g_logger.warning(str(e))
                g_logger.exception(e)


if __name__ == "__main__":
    start_time = time.localtime(int(time.time())-60*86400)

    tm_mday = 1
    if start_time.tm_mday<=15:
        tm_mday = 1
    else:
        tm_mday = 16

    dt = datetime.datetime(start_time.tm_year, start_time.tm_mon, tm_mday, 0, 0, 0)
    start_ts = int(dt.timestamp())
    #week零点
    start_ts = start_ts - ((start_ts - 316800) % 604800)

    #加载数据
    g_zen_kline_data.LoadTradeDays()
    g_zen_kline_data.LoadAllTradeTimeSection()
    g_zen_kline_data.LoadAllSecurities()
    for code in g_zen_kline_data.sec_insert_ts_idx.keys():
        ret = g_zen_kline_data.LoadSecuritiesKlineData(code, '1min', start_ts)
        if ret < 0:
            g_logger.warning("LoadSecuritiesKlineData ret=%d", ret)
            continue

    zen_data_svr = ZenDataSvr('ZenDataSvr Thread')
    zen_data_svr.start()

    zen_data_sync = ZenDataSync('ZenDataSync Thread')
    zen_data_sync.start()

    while True:
        time.sleep(3600)

