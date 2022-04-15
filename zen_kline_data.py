
# -*- coding:utf-8 -*-
#! python3

from pandas import DataFrame, Series
import pandas as pd
import numpy as np

import time
import datetime
import threading
import sys
import os
import mysql.connector
import pymysql
import sqlalchemy
from sqlalchemy import create_engine
from queue import Queue,LifoQueue,PriorityQueue

import configparser

from zd_logging import g_logger
from utils import *
from jqdatasdk import *


#从mysql中读取数据类
class ZenKlineData:
    mydb = None
    connect_info = None
    engine = None
    lock = None
    sync_queue = None
    securities_type=''
    all_securities = dict()
    all_trade_time_sections = dict()
    dict_trade_days = dict()
    list_trade_days = list()
    sec_insert_ts_idx = dict() #key:code value:last insert ts
    sec_trade_times = dict()  #key:code value:trade times
    sec_kline_data = dict()  #key:code value:df
    def __init__(self, securities_type, path):
        self.securities_type = securities_type
        self.lock = threading.Lock()
        self.sync_queue = Queue()
        if path=='':
            g_logger.info("ZenKlineData init, path is null!")

        try:
            g_logger.info('cfg_path=%s', path)
            cf = configparser.ConfigParser()
            cf.read(path)
            host = cf.get("mysql", "host")
            username = cf.get("mysql", "username")
            password = cf.get("mysql", "password")
            dbname = cf.get("mysql", "dbname")
            port = cf.get("mysql", "port")
            self.mydb = mysql.connector.connect(
                host=host,
                port=port,
                user=username,
                passwd=password,
                database=dbname
            )
            self.connect_info = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(username, password, host, port, dbname) #1
            self.engine = create_engine(self.connect_info)

            codes = cf.get("securities", "codes")
            arr_codes = codes.split(',')
            for code in arr_codes:
                if code != '':
                    self.sec_insert_ts_idx[code] = 1
        except Exception as e:
            g_logger.warning(str(e))
            g_logger.exception(e)

    def __del__(self):
        self.mydb.close()

    #加载所有的交易时间段
    def LoadAllTradeTimeSection(self):
        g_logger.info('LoadAllTradeTimeSection begin! securities_type=%s', self.securities_type)
        db_name = ''
        if self.securities_type=='stock':
            db_name = 'gp'
        elif self.securities_type=='index':
            db_name='idx'
        elif self.securities_type=='futures':
            db_name='futures'

        mycursor = self.mydb.cursor()
        try:
            sec_sql = "SELECT id, start_time, end_time FROM " + db_name + ".trade_time_section"
            mycursor.execute(sec_sql)
            # 获取所有记录列表
            results = mycursor.fetchall()
            for row in results:
                tss = []
                tid = int(row[0])
                start_time = row[1]
                start_times = start_time.split(':')
                hour = int(start_times[0])
                min = int(start_times[1])
                start_ts = hour*3600 + min*60
                tss.append(start_ts)

                end_time = row[2]
                end_times = end_time.split(':')
                hour = int(end_times[0])
                min = int(end_times[1])
                end_ts = hour*3600 + min*60
                tss.append(end_ts)
                self.all_trade_time_sections[tid] = tss
            g_logger.debug("len(all_trade_time_sections)=%d", len(self.all_trade_time_sections))
        except Exception as e:
            g_logger.warning(str(e))
            g_logger.exception(e)


    #加载所有的gp代码
    def LoadAllSecurities(self):
        g_logger.info('LoadAllSecurities begin! securities_type=%s', self.securities_type)
        db_name = ''
        if self.securities_type=='stock':
            db_name = 'gp'
        elif self.securities_type=='index':
            db_name='idx'
        elif self.securities_type=='futures':
            db_name='futures'

        mycursor = self.mydb.cursor()
        try:
            sec_sql = "SELECT id, code, start_date, end_date,IFNULL(trade_times, '') FROM " + db_name + ".securities"
            mycursor.execute(sec_sql)
            # 获取所有记录列表
            results = mycursor.fetchall()
            for row in results:
                oneSecurities = dict()
                oneSecurities['id'] = int(row[0])
                oneSecurities['code'] = row[1]
                oneSecurities['start_date'] = row[2]
                oneSecurities['end_date'] = row[3]

                str_trade_times = row[4]
                arr_trade_times = str_trade_times.split(',')
                trade_times = []
                for trade_time in arr_trade_times:
                    if trade_time=='':
                        continue
                    trade_times.append(int(trade_time))
                oneSecurities['trade_times'] = trade_times

                if row[1] in self.sec_insert_ts_idx:
                    self.sec_trade_times[row[1]] = trade_times

                self.all_securities[row[1]] = oneSecurities
                # g_logger.debug("code=%s, securities=%s", row[1], str(oneSecurities))
            g_logger.debug("securities length=%d", len(self.all_securities))
        except Exception as e:
            g_logger.warning(str(e))
            g_logger.exception(e)

    #加载所有的交易日
    def LoadTradeDays(self):
        g_logger.info('LoadTradeDays begin!')
        mycursor = self.mydb.cursor()
        try:
            sec_sql = "SELECT day FROM gp.gp_trade_days where day>='2021-01-01' ORDER BY day ASC"
            mycursor.execute(sec_sql)
            # 获取所有记录列表
            results = mycursor.fetchall()
            for row in results:
                self.dict_trade_days[row[0]]=1
                self.list_trade_days.append(row[0])
            g_logger.debug("trade_days length=%d", len(self.list_trade_days))
        except Exception as e:
            g_logger.warning(str(e))
            g_logger.exception(e)

    #加载code的kline数据
    def LoadSecuritiesKlineData(self, code, period, from_ts):
        g_logger.info('LoadSecuritiesKlineData begin! code=%s, period=%s', code, period)
        db_name = ''
        if self.securities_type=='stock':
            db_name = 'gp'
        elif self.securities_type=='index':
            db_name='idx'
        elif self.securities_type=='futures':
            db_name='futures'

        #先找出gp_id
        if code not in self.all_securities:
            g_logger.warning("code:%s not in all_securities", code)
            return -1

        gp_id = self.all_securities[code]["id"]

        codes = code.split(".")
        if len(codes) != 2:
            g_logger.warning("error code:%s", code)
            return -2

        try:
            table_name = period + "_prices_" + codes[0][-2:]
            if self.securities_type=='futures':
                table_name = period + "_prices_" + codes[0][0:2]
                table_name = table_name.lower()
            sec_sql = "SELECT ts, open, high, low, close, volume, money, factor FROM " + db_name + "." + table_name \
                      + " WHERE gp_id='" + str(gp_id) + "' AND ts>='" + str(from_ts) + "' ORDER BY ts ASC"
            df = pd.read_sql(sql=sec_sql, con=self.engine)
            df['OriginalIndex'] = df.index
            df.rename(columns={'ts':'Ts','open':'Open', 'high':'High', 'low':'Low', 'close':'Close', 'volume':'Volume', 'money':'Amount', 'factor':'Factor'}, inplace=True)
            g_logger.debug("pd.read_sql end")

            df['Ts'] = df['Ts'].astype('int32')
            df = df.reindex(columns=['Ts','Open','High','Low','Close', 'Volume', 'Amount', 'Factor'])
            kline_len = len(df)
            g_logger.debug("kline length=%d", kline_len)
            if kline_len>0:
                self.sec_kline_data[code] = df
                self.sec_insert_ts_idx[code] = df.loc[kline_len-1, 'Ts']
            return 0
        except Exception as e:
            g_logger.warning(str(e))
            g_logger.exception(e)
            return -3

    #插入kline
    def InsertKlineToDf(self, code, period, klines, is_lock=True):
        if is_lock:
            self.lock.acquire()
        try:
            klines_len = len(klines.index)
            for i in range(klines_len):
                kline_ts = int((klines.loc[i, 'date'].tz_localize(tz='Asia/Shanghai').timestamp()))
                kline_ts -= 60

                all_kline_len = len(self.sec_kline_data[code].index)
                last_kline_ts = self.sec_kline_data[code].loc[all_kline_len-1, 'Ts']
                if kline_ts<last_kline_ts:
                    continue

                row = {}
                row['Ts']     =  kline_ts
                row['Open']   =  klines.loc[i, 'open']
                row['High']   =  klines.loc[i, 'high']
                row['Low']    =  klines.loc[i, 'low']
                row['Close']  =  klines.loc[i, 'close']
                row['Volume'] =  klines.loc[i, 'volume']
                row['Amount'] =  klines.loc[i, 'money']
                row['Factor'] =  klines.loc[i, 'factor']
                if kline_ts==last_kline_ts:
                    # self.sec_kline_data[code].drop([-1], axis=0, inplace=True)
                    self.sec_kline_data[code].loc[all_kline_len-1, ('Ts','Open','High','Low','Close','Volume','Amount','Factor')] = \
                        [row['Ts'], row['Open'],row['High'],row['Low'],row['Close'],row['Volume'],row['Amount'],row['Factor']]
                    g_logger.info('Update Df, code=%s, period=%s, ts=%d, strtime=%s', code, period, kline_ts,
                                  datetime.datetime.fromtimestamp(kline_ts).strftime('%Y-%m-%d %H:%M:%S'))
                elif kline_ts>last_kline_ts:
                    newdf=pd.DataFrame(row, index=[all_kline_len])
                    self.sec_kline_data[code] = self.sec_kline_data[code].append(newdf, ignore_index=True)
                    g_logger.info('Insert Df, code=%s, period=%s, ts=%d, strtime=%s', code, period, kline_ts,
                                  datetime.datetime.fromtimestamp(kline_ts).strftime('%Y-%m-%d %H:%M:%S'))
                    # self.sec_kline_data[code].loc[-1]=row
        except Exception as e:
            g_logger.warning(str(e))
            g_logger.exception(e)
            return -1
        finally:
            if is_lock:
                self.lock.release()
            return 0

    #获取kline
    def JudgeContinueKlines(self, code, last_ts, this_ts):
        if (this_ts-last_ts)==60:
            return True

        last_diff_ts = ((last_ts + 8 * 3600) % 86400)
        this_diff_ts = ((this_ts + 8 * 3600) % 86400)
        trade_times = self.all_securities[code]['trade_times']
        if len(trade_times)<2:
            return False

        for idx in range(len(trade_times)):
            section1 = self.all_trade_time_sections[trade_times[(idx-1+len(trade_times))%len(trade_times)]]
            section2 = self.all_trade_time_sections[trade_times[idx]]
            if last_diff_ts==(section1[1]-60) and this_diff_ts==section2[0]:
                return True

    #插入kline
    def InsertKlineToDb(self, code, period, ts, open, high, low, close, volume, money, factor):
        db_name = ''
        if self.securities_type=='stock':
            db_name = 'gp'
        elif self.securities_type=='index':
            db_name = 'idx'
        elif self.securities_type=='futures':
            db_name = 'futures'

        #先找出sec_id
        if code not in self.all_securities:
            g_logger.warning("code:%s not in all_securities", code)
            return -1

        sec_id = self.all_securities[code]["id"]
        codes = code.split(".")
        if len(codes) != 2:
            g_logger.warning("error code:%s", code)
            return

        tableName = period + "_prices_" + codes[0][-2:]
        if self.securities_type=='futures':
            tableName = period + "_prices_" + codes[0][0:2]
            tableName = tableName.lower()

        mycursor = self.mydb.cursor()
        str_sql = "INSERT INTO " + db_name + "." + tableName + "(gp_id, ts, open, high, low, close, volume, money, factor) VALUES "
        str_sql += "("
        str_sql += str(sec_id)
        str_sql += ","
        str_sql += str(ts)
        str_sql += ","
        str_sql += "'%.2f'" % round(open, 2)
        str_sql += ","
        str_sql += "'%.2f'" % round(high, 2)
        str_sql += ","
        str_sql += "'%.2f'" % round(low, 2)
        str_sql += ","
        str_sql += "'%.2f'" % round(close, 2)
        str_sql += ","
        str_sql += "'%.2f'" % round(volume, 2)
        str_sql += ","
        str_sql += "'%.2f'" % round(money, 2)
        str_sql += ","
        str_sql += "'%.6f'" % round(factor, 6)
        str_sql += ")"
        try:
            mycursor.execute(str_sql)
            self.mydb.commit()
            g_logger.info('Insert Db, code=%s, period=%s, ts=%d, strtime=%s', code, period,
                          ts, datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))
        except Exception as e:
            self. mydb.rollback()
            g_logger.warning("str_sql=%s", str_sql)
            g_logger.warning(str(e))
            g_logger.exception(e)


    #获取kline
    def GetKlines(self, code, period, from_ts, to_ts):
        g_logger.info('GetKlines begin! code=%s, period=%s, from_ts=%d, to_ts=%d', code, period, from_ts, to_ts)

        if code not in self.all_securities:
            g_logger.warning("code:%s not in all_securities", code)
            return None

        codes = code.split(".")
        if len(codes) != 2:
            g_logger.warning("error code:%s", code)
            return None

        df = None
        self.lock.acquire()
        try:
            sec_data_len = len(self.sec_kline_data[code].index)
            from_idx = -1
            to_idx = -1
            for j in range(sec_data_len-1, -1, -1):
                sec_ts = self.sec_kline_data[code].loc[j, 'Ts']
                if sec_ts>=from_ts and sec_ts<=to_ts:
                    if from_idx==-1 and to_idx==-1:
                        from_idx = j
                        to_idx = j
                    elif j<from_idx:
                        from_idx = j
                elif sec_ts<from_ts:
                    break

            if from_idx==-1 or to_idx==-1:
                return None

            df = self.sec_kline_data[code].loc[from_idx:to_idx, :]
            return df
        except Exception as e:
            g_logger.warning(str(e))
            g_logger.exception(e)
            return None
        finally:
            self.lock.release()
            return df

    #同步缺失的Kline
    def SyncKlines(self):
        # g_logger.info('SyncKlines 1')
        self.lock.acquire()
        try:
            for code, trade_times in self.sec_trade_times.items():
                # g_logger.info('SyncKlines code：%s', code)
                now_ts = int(time.time())
                now_ts -= now_ts%60
                try:
                    last_kline = self.sec_kline_data[code].iloc[-1]
                    last_ts = last_kline['Ts']
                    kline_count = self.GetKlineCount(code, int(last_ts), now_ts)
                    if kline_count>0:
                        end_datetime = datetime.datetime.fromtimestamp(now_ts+60)
                        # g_logger.info('before get_bars, code=%s', code)
                        bars = get_bars([code], kline_count, unit='1m',
                                          fields=['date', 'open', 'close', 'high', 'low', 'volume', 'money', 'factor'],
                                          include_now=True, end_dt=end_datetime, fq_ref_date=None, df=True)
                        # g_logger.info('after get_bars, code=%s', code)
                        klines = bars.loc[code, :]
                        # idx = bars.IndexSlice
                        # klines = bars.loc[idx[code,:], :]
                        self.InsertKlineToDf(code, "1min", klines, False)

                    last_insert_ts = self.sec_insert_ts_idx[code]
                    klines_len = len(self.sec_kline_data[code].index)
                    from_idx = -1
                    for j in range(klines_len-1, -1, -1):
                        sec_ts = self.sec_kline_data[code].loc[j, 'Ts']
                        if sec_ts==last_insert_ts:
                            from_idx = j
                            break

                    if from_idx==-1:
                        continue

                    for j in range(from_idx+1, klines_len):
                        ts = self.sec_kline_data[code].loc[j, 'Ts']
                        if self.JudgeContinueKlines(code, last_insert_ts, ts) is False:
                            g_logger.warning("not continue klines, last_insert_ts=%d, ts=%d", last_insert_ts, ts)
                            break

                        if j==(klines_len-1) and (now_ts-ts)<120:
                            continue

                        open = self.sec_kline_data[code].loc[j, 'Open']
                        high = self.sec_kline_data[code].loc[j, 'High']
                        low = self.sec_kline_data[code].loc[j, 'Low']
                        close = self.sec_kline_data[code].loc[j, 'Close']
                        volume = self.sec_kline_data[code].loc[j, 'Volume']
                        amount = self.sec_kline_data[code].loc[j, 'Amount']
                        factor = self.sec_kline_data[code].loc[j, 'Factor']
                        self.InsertKlineToDb(code, "1min", ts, open, high, low, close, volume, amount, factor)
                        last_insert_ts = ts
                        self.sec_insert_ts_idx[code] = ts
                except Exception as e:
                    g_logger.warning(str(e))
                    g_logger.exception(e)
                    return -1
        except Exception as e:
            g_logger.warning(str(e))
            g_logger.exception(e)
            return -1
        finally:
            self.lock.release()
            return 0

    def GetKlineCount(self, code, last_ts, this_ts):
        try:
            last_dt = datetime.datetime.fromtimestamp(last_ts)
            this_dt = datetime.datetime.fromtimestamp(this_ts)
            last_day = last_dt.strftime('%Y-%m-%d')
            this_day = this_dt.strftime('%Y-%m-%d')
            trade_times =  self.sec_trade_times[code]
            count = 0
            disp_this_ts = this_ts

            if last_day==this_day:
                last_section_idx = -1
                this_section_idx = -1

                for idx, trade_section_idx in enumerate(trade_times):
                    trade_time = self.all_trade_time_sections[trade_section_idx]
                    start_section_ts = trade_time[0] + DayZeroTs(this_ts)
                    end_section_ts = trade_time[1] + DayZeroTs(this_ts)
                    if last_ts>=start_section_ts and last_ts<end_section_ts:
                        last_section_idx = idx

                    if this_ts>=start_section_ts and this_ts<end_section_ts:
                        this_section_idx = idx
                    elif this_ts<start_section_ts and this_section_idx==-1:
                        this_section_idx = idx-1
                        trade_time = self.all_trade_time_sections[trade_times[this_section_idx]]
                        disp_this_ts = trade_time[1] + DayZeroTs(this_ts) - 60

                if this_section_idx==-1:
                    this_section_idx = len(trade_times)-1
                    trade_time = self.all_trade_time_sections[this_section_idx]
                    disp_this_ts = trade_time[1] + DayZeroTs(this_ts) - 60

                if last_section_idx==this_section_idx and last_section_idx != -1:
                    count = int((disp_this_ts-last_ts)/60)
                else:
                    for idx in range(last_section_idx, this_section_idx+1):
                        trade_section_idx = trade_times[idx]
                        trade_time = self.all_trade_time_sections[trade_section_idx]
                        if idx==last_section_idx:
                            count += int((trade_time[1]+ DayZeroTs(this_ts)-60-last_ts)/60)
                        elif idx==this_section_idx:
                            count += int((this_ts- DayZeroTs(this_ts) - trade_time[0] + 60)/60)
                        else:
                            count += int((trade_time[1] - trade_time[0])/60)

                return count+1

            day_count=0
            bStart=False
            for idx, trade_day in enumerate(self.list_trade_days):
                if last_day==trade_day:
                    bStart = True
                if bStart:
                    day_count += 1
                if this_day==trade_day and bStart is True:
                    count = day_count*480
                    return count
        except Exception as e:
            g_logger.warning(str(e))
            g_logger.exception(e)
            return -1


def CheckSameDay(ts1, ts2):
    ts1_zero = ts1 - (ts1+28800)%86400
    ts2_zero = ts2 - (ts2+28800)%86400
    if (ts1_zero==ts2_zero):
        return True
    else:
        return False

def DayZeroTs(ts1):
    return ts1 - (ts1+28800)%86400

if __name__ == "__main__":
    zen_ms_data = ZenKlineData('index', 'config.ini')
    zen_ms_data.LoadAllSecurities()
    zen_ms_data.LoadSecuritiesKlineData('000001.XSHG', '1min', 0)

