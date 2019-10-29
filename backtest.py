# -*- coding: utf-8 -*-
#from __future__ import unicode_literals
import csv
import os
import time
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
from mpl_finance import candlestick_ohlc
import matplotlib.pyplot as plt
#import matplotlib.ticker as ticker
from matplotlib import ticker as mticker
from matplotlib.dates import num2date, date2num
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter,MonthLocator, WeekdayLocator,DayLocator, WEDNESDAY
import matplotlib.dates
import pandas
import numpy as np
import pandas as pd
import requests
import inspect
import tdcc_dist
import eps
import good_stock
from inspect import currentframe, getframeinfo

import seaborn as sns
import matplotlib as mpl
import stock_comm as comm
from matplotlib import ticker
import mpl_finance
import backtrader as bt
import scipy.signal as signal 
from stocktool import comm as comm1
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)

class order:
    #def __init__(self, timestamp, symbol, qty, is_buy,is_market_order, price=0):
    def __init__(self, stock_id,signal_date):
        self.stock_id=stock_id
        self.signal_date = signal_date
        self.buy_price = 0
        self.buy_date = 0
        self.sell_date = 0
        self.error=0
        self.protect=0
        #self.qty = 0
        self.action='buy'
        df1=comm.get_stock_df_by_startdate(self.stock_id,comm.get_prev_month_date(self.signal_date,3))
        ma_list = [10,21]
        for ma in ma_list:
            df1['MA_' + str(ma)] = df1['close'].rolling(window=ma,center=False,axis=0).mean()
            
        self.lastpeak=0 #comm.get_last_peak(df1,signal_date)
        self.lastvalley=0 #comm.get_last_valley(df1,signal_date)
        #print(lno(),self.lastpeak,self.lastvalley)
        self.holddays=0
        self.df=df1
        self.profit=0.0  
        self.stoploss1=0.0  
        self.stoploss2=0.0  

        
class backtest:
    def __init__(self,startdate,enddate,sellmode=0,strategy=0):
        self.strategy=strategy
        """
        mode 0: 40天 無停損 
        mode 1: 均線停損停利法 獲利未過20%,跌破10日出清,獲利超過20% ,跌破21日 出清
        mode 2: 前波低為防守價,過前波高,以前波高為防守價 待寫
        mode 2 k線停損停利 長紅突破區間,不破昨日低,獲利超過20% ,守前波低
        """
        self.sellmode=sellmode
        self.order_list=[]
        self.history_list =[]
        self.wins=0
        self.profit=0.0
        self.startdate=startdate
        self.enddate=enddate
        
        
    def get_date_index(self,df,rundate):
        #print(lno(),df)
        pos_list=df.index[(df.loc[:,"date"] == np.datetime64(rundate))]
        if len(pos_list)!=1:
            print(lno(),rundate,df.head(1),"no date",pos_list)
            #print(lno(),self.df)
            return None 
        return pos_list[0]
    def handle_buy_action(self,rundate,order,pos):
        order.buy_price=order.df.iloc[pos]['open']
        order.action='hold'
        order.buy_date=rundate
        #print(lno(),self.order_df.iloc[i])
    def order_stoploss_ma(self,rundate,order,pos):
        if order.df.iloc[pos]['close']<order.stoploss1:
            if order.profit>=1.2 and order.df.iloc[pos]['close']>=order.df.iloc[pos]['MA_21']:
                #print(lno(),"check",self.stock_id,rundate)
                order.protect=order.protect+1
            else:
                order.action='sell'
                
    def order_stoploss_kline(self,rundate,order,pos):
        
        if order.df.iloc[pos]['close']<order.stoploss2:
            if order.profit>=1.2:
                order.lastvalley = comm.get_last_peak(order.df,rundate)
                if order.df.iloc[pos]['close']<order.lastvalley:
                    order.action='sell'
            else:        
                order.action='sell'
                
    def handle_hold_action(self,rundate,order,pos):
       
        order.profit=comm1.calc_profit(order.buy_price,order.df.iloc[pos]['close'])
        order.holddays=order.holddays+1
        #print(lno(),self.order_df.iloc[i])
        if self.sellmode==0:
            if order.holddays>=41:
                order.action='sell'
            elif order.df.iloc[pos]['close']< order.buy_price*0.9:
                order.action='sell'
        elif self.sellmode==1: 
            self.order_stoploss_ma(rundate,order,pos)
        elif self.sellmode==2:
            self.order_stoploss_kline(rundate,order,pos)
                    
    def handle_sell_action(self,rundate,order,pos):
        order.profit=comm1.calc_profit(order.buy_price,order.df.iloc[pos]['open'])
        order.action='finish'
        order.sell_date=rundate
        order.df=None
        #print(lno(),self.order_df.iloc[i])
    def add_history_list(self,rundate): 
        list=[rundate,len(self.order_list),self.wins/len(self.order_list)*100,self.profit,self.profit/len(self.order_list)]

        self.history_list.insert(0,list)
        print(lno(),list)
        
    def run(self,rundate):    
        if len(self.order_list)==0:
            return
        #print(lno(),self.order_list) 
        self.wins=0
        self.profit=0.0
        for order in self.order_list:
            data_df=order.df
            if order.action=='finish':
                self.profit=self.profit+order.profit
                if order.profit>=1:
                    self.wins=self.wins+1
                continue
            pos=self.get_date_index(data_df,rundate)
            if pos==None:
                order.error=order.error+1
                if order.error>=30:
                    order.action='finish'
                    order.df=None
                continue
            order.error=0
            if order.action=='buy':
                self.handle_buy_action(rundate,order,pos)
            if order.action=='hold':    
                self.handle_hold_action(rundate,order,pos)
            elif  order.action=='sell':       
                self.handle_sell_action(rundate,order,pos)
            else:
                print(lno(),"why see this",order.action)
            if self.sellmode==1 and order.action!='finish':        
                order.stoploss1=order.df.iloc[pos]['MA_21']
            elif self.sellmode==2 and order.action!='finish':
                order.stoploss2=order.df.iloc[pos]['low']    
            self.profit=self.profit+order.profit
            if order.profit>=1:
                self.wins=self.wins+1    
        self.add_history_list(rundate)

    def gen_buy_order(self,rundate):
        df1=good_stock.get_good_stock_m1(rundate)
        if len(df1)!=0:
            for i in df1['證券代號'].values.tolist():
                if len(i)!=4:
                    continue
                self.order_list.append(order(i,rundate))  

    def report(self):
        """
        產生 history_df to out/backtest/xxx.csv
        order_list==>order_df
        """
        old_width = pd.get_option('display.max_colwidth')
        pd.set_option('display.max_colwidth', -1)
        
        columns=['日期','買進次數','勝率','總獲利','平均獲利']
        df=pd.DataFrame(self.history_list, columns=columns)
        
        df=df.round({'勝率': 2,'總獲利': 3,'平均獲利': 5})
        print(lno(),df)
        filen='out/backtest/%d%02d%02d-%d%02d%02d_mode%d_strategy%d_history.html'%(self.startdate.year,self.startdate.month,self.startdate.day,self.enddate.year,self.enddate.month,self.enddate.day,self.sellmode,self.strategy)
        comm.check_dst_folder(os.path.dirname(filen))
        df.to_html(filen,escape=False,index=False,sparsify=True,border=2,index_names=False)
        list=[]
        for order in self.order_list:
            if order.profit!=0:
                list.append([order.stock_id,order.buy_date,order.buy_price,order.action,order.profit,order.holddays])
        columns=['股票代號','買進日期','買進價格','目前狀態','目前獲利','持有天數']
        df=pd.DataFrame(list, columns=columns)
        df=df.round({'目前獲利': 5})
        filen='out/backtest/%d%02d%02d-%d%02d%02d_mode%d_strategy%d_order.html'%(self.startdate.year,self.startdate.month,self.startdate.day,self.enddate.year,self.enddate.month,self.enddate.day,self.sellmode,self.strategy)
        df.to_html(filen,escape=False,index=False,sparsify=True,border=2,index_names=False)
        pd.set_option('display.max_colwidth', old_width)
    def start(self):
        day=0
        nowdate=self.startdate
        while   nowdate<=self.enddate :
            if comm.check_work_date(nowdate)==True:
                bt.run(nowdate)  
                bt.gen_buy_order(nowdate)
            day=day+1    
            nowdate = startdate + relativedelta(days=day) 
    
if __name__ == '__main__':
    sns.set()
    mpl.rcParams['font.family'] = 'sans-serif'
    mpl.rcParams['font.sans-serif'] = 'SimHei'
  #df=kline.get_tse_kline_df(startdate,enddate)
    if sys.argv[1]=='1' :
        pass
    elif sys.argv[1]=='-t' :
        if len(sys.argv)==4 :
            #stock_no=sys.argv[2]
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            
            tStart = time.time()
            stoploss_list=[0,1,2]
            #stoploss_list=[0]
            for mode in stoploss_list:
                bt=backtest(startdate,enddate,sellmode=mode,strategy=0)
                bt.start()
                bt.report()    
            tEnd = time.time()
    
            print ("It cost %.3f sec" % (tEnd - tStart))    

        else :
            
            print (lno(),'func -t stock_no date')  
    elif sys.argv[1]=='-t2' :
            print(lno())
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            print(lno())
            test=comm.exchange_data()
            print(lno())
            test.save_sql(startdate)
            print(lno())        
    
