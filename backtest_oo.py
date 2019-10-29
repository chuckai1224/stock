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
TWIIPATH='csv/twii'
TSE_KLINE_PATH='csv/tse_kline'
def check_dst_folder(dstpath):
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath)  
   

def start_backtest(stock_no,startdate):
    """
    賣出法 1.收盤跌破昨日低 賣出
    2.跌破買進價10%賣出
    3.收盤跌破前箱頂,賣出
    箱轉折點計算 空間2%,時間2日
    
    """
    ## stock_no buy signal gen at startdate ,buy next day open price 
    #buy_order=Order(startdate,stock_no,1000,is_buy=True)
    #print(lno(),buy_order.is_buy)
    cnt=60
    tStart = time.time()
    df1=comm.get_stock_df_by_startdate(stock_no,comm.get_prev_month_date(startdate,3))
    ma_list = [5,10,21]
    for ma in ma_list:
        df1['MA_' + str(ma)] = df1['close'].rolling(window=ma,center=False,axis=0).mean()
    tEnd = time.time()
    
    print ("It cost %.3f sec" % (tEnd - tStart))
    print (df1)
    df=df1[(df1.loc[:,"date"] == np.datetime64(startdate))]
    if len(df)!=1:
        print(lno(),startdate,"no date")
    pos=df.index.values[0]
   
    buy_price=df1.iloc[pos+1]['open']
    if len(df1)>=40+pos:
        sell_price=df1.iloc[pos+41]['close']
    else:
        sell_price=df1.iloc[-1]['close']
    print(lno(),'B:',buy_price,'S:',sell_price)    
    ## mode 2
    Stoploss2=df1.iloc[pos]['low']
    i=0
    for i in range(pos+1,len(df1)):
        #print(lno(),i,'收:',df1.iloc[i]['close'],'損:',Stoploss2)
        if df1.iloc[i]['close']>=Stoploss2:
            Stoploss2=df1.iloc[i]['low']
        else:
            if df1.iloc[i]['close']/buy_price>=2 and df1.iloc[i]['MA_21']>=df1.iloc[i-1]['MA_21']:
                Stoploss2=df1.iloc[i]['low']
            elif df1.iloc[i]['close']/buy_price>=1.5 and df1.iloc[i]['MA_10']>=df1.iloc[i-1]['MA_10']:
                Stoploss2=df1.iloc[i]['low']
            elif df1.iloc[i]['close']/buy_price>=1.25 and df1.iloc[i]['MA_10']>=df1.iloc[i-1]['MA_10']:
                Stoploss2=df1.iloc[i]['low']
            else:
                if i+1<len(df1):
                    sell_price1=df1.iloc[i+1]['open']
                else:    
                    sell_price1=df1.iloc[i]['close']
                break
    print(lno(),'B:',buy_price,'S:',sell_price1)    
        
    return 
    
   

class Order:
    #def __init__(self, timestamp, symbol, qty, is_buy,is_market_order, price=0):
    def __init__(self, stock_id,signal_date, is_buy,stop_loss_mode=0):
        self.stock_id=stock_id
        self.signal_date = signal_date
        self.buy_price = 0
        self.buy_date = 0
        self.sell_date = 0
        self.error=0
        self.protect=0
        
        self.stop_loss_mode = stop_loss_mode
        #self.qty = 0
        self.action='buy'
        self.transaction_tax=0.001425*0.5
        self.tax_payment=0.003
        df1=comm.get_stock_df_by_startdate(self.stock_id,comm.get_prev_month_date(self.signal_date,3))
        ma_list = [10,21]
        for ma in ma_list:
            df1['MA_' + str(ma)] = df1['close'].rolling(window=ma,center=False,axis=0).mean()
            
        self.lastpeak=0 #comm.get_last_peak(df1,signal_date)
        self.lastvalley=0 #comm.get_last_valley(df1,signal_date)
        #print(lno(),self.lastpeak,self.lastvalley)
        self.buy_date_index=df1.index[(df1.loc[:,"date"] == np.datetime64(signal_date))][0]
        self.df=df1
        self.profit_on_account=0.0

    def get_date_index(self,rundate):
        pos_list=self.df.index[(self.df.loc[:,"date"] == np.datetime64(rundate))]
        if len(pos_list)!=1:
            print(lno(),rundate,self.stock_id,"no date",pos_list)
            #print(lno(),self.df)
            return None
        return pos_list[0]
    def calc_profit(self,buy_price,sell_price):
        fix_buy_price=buy_price*(1+self.transaction_tax)
        fix_sell_price=sell_price*(1-self.tax_payment-self.transaction_tax)
        return fix_sell_price/fix_buy_price
    def simulate(self,rundate):
        if rundate==self.signal_date:
            ##還沒買入
            return 0
        if  self.action=='finish': 
            return self.profit_on_account    
        now_pos=self.get_date_index(rundate)
        if now_pos==None:
            self.error=self.error+1
            if self.error>=30:
                self.action='finish'
                self.df=None
            return 0
        self.error=0    
        #print(lno(),self.action,now_pos)
        if self.action=='buy':
            #print(self.df)
            self.buy_price=self.df.iloc[now_pos]['open']
            self.action='hold'
            self.buy_date=rundate
            self.profit_on_account=self.calc_profit(self.buy_price,self.df.iloc[now_pos]['close'])
        elif  self.action=='hold':
            self.profit_on_account=self.calc_profit(self.buy_price,self.df.iloc[now_pos]['close'])
            if self.stop_loss_mode==0:
                if now_pos-self.buy_date_index>=40:
                    self.action='finish'
                    self.df=None
                    return self.profit_on_account
                if self.df.iloc[now_pos]['close']< self.buy_price*0.9:
                    self.action='sell'
            elif self.stop_loss_mode==1:
                if self.df.iloc[now_pos]['close']<self.stoploss1:
                    if self.profit_on_account>=1.5 and self.df.iloc[now_pos]['MA_21']>=self.df.iloc[now_pos-1]['MA_21']:
                        #print(lno(),"check",self.stock_id,rundate)
                        self.protect=self.protect+1
                    elif self.profit_on_account>=1.2 and self.df.iloc[now_pos]['MA_10']>=self.df.iloc[now_pos-1]['MA_10']:
                        #print(lno(),"check",self.stock_id,rundate)
                        self.protect=self.protect+1
                    else:
                        self.action='sell'
            elif self.stop_loss_mode==2:
                if self.profit_on_account>=1.2:
                    self.lastvalley = comm.get_last_peak(self.df,rundate)
                    if self.df.iloc[now_pos]['close']<self.lastvalley:
                        self.action='sell'
                elif self.df.iloc[now_pos]['close']<self.stoploss2:
                    self.action='sell'
                
            else:
                print(lno(),"need to add")
        elif  self.action=='sell':   
            if self.stop_loss_mode==1 or self.stop_loss_mode==0 or self.stop_loss_mode==2:
                self.profit_on_account=self.calc_profit(self.buy_price,self.df.iloc[now_pos]['open'])
                self.action='finish'
                self.sell_date=rundate
                self.df=None
                return self.profit_on_account
            else:
                print(lno(),"why see here")

        else:    
            print(lno(),"why see here")
        if self.stop_loss_mode==1:        
            self.stoploss1=self.df.iloc[now_pos]['MA_21']
        elif self.stop_loss_mode==2:
            self.stoploss2=self.df.iloc[now_pos]['low']
       
        return self.profit_on_account
    def show(self):
        print(lno(),self.stock_id,self.signal_date,self.df)
def gen_buy_order(stock_no,date,stop_loss_mode):
    buy_order=Order(stock_no,date,is_buy=True,stop_loss_mode=stop_loss_mode)
    #buy_order.run()
    #print(lno(),buy_order.show())
    return buy_order

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
        
class backtest:
    def __init__(self,startdate,enddate,sellmode=0,strategy=0):
        self.strategy=strategy
        """
        mode 0: 40天 無停損 
        mode 1: 不破昨日低 獲利 超過25% 以10ma 斜率>0  50% 以21ma
        mode 2: 前波低為防守價,過前波高,以前波高為防守價 待寫
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
            if order.df.iloc[pos]['close']<order.stoploss1:
                if order.profit>=1.5 and order.df.iloc[pos]['MA_21']>=order.df.iloc[pos-1]['MA_21']:
                    #print(lno(),"check",self.stock_id,rundate)
                    order.protect=order.protect+1
                elif order.profit>=1.2 and order.df.iloc[pos]['MA_10']>=order.df.iloc[pos-1]['MA_10']:
                    #print(lno(),"check",self.stock_id,rundate)
                    order.protect=order.protect+1
                else:
                    order.action='sell'
            pass
        elif self.sellmode==2:
                if order.profit>=1.2:
                    order.lastvalley = comm.get_last_peak(order.df,rundate)
                    if order.df.iloc[pos]['close']<order.lastvalley:
                        order.action='sell'
                elif order.df.iloc[pos]['close']<order.stoploss2:
                    order.action='sell'    
    def handle_sell_action(self,rundate,order,pos):
        order.profit=comm1.calc_profit(order.buy_price,order.df.iloc[pos]['open'])
        order.action='finish'
        order.sell_date=rundate
        order.df=None
        #print(lno(),self.order_df.iloc[i])
    def add_history_list(self,rundate): 
        list=[rundate,len(self.order_list),self.wins/len(self.order_list)*100,self.profit,self.profit/len(self.order_list)]
        self.history_list.append(list)
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
            if self.sellmode==1:        
                order.stoploss1=order.df.iloc[pos]['MA_21']
            elif self.sellmode==2:
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
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        selectdate=startdate
        #df=get_tse_df_from_csv(startdate,enddate) 
        stock_no='6152'
        mode=21
        filen='out/%s_%d.png'%(stock_no,mode)
        test2(stock_no,enddate,21,filen)
        test3(stock_no,enddate,filen)
        
    elif sys.argv[1]=='-d' :
        print (lno(),len(sys.argv))
        if len(sys.argv)==2 :
            today= datetime.today().date()
            enddate=datetime(year=today.year,month=today.month,day=today.day,)
            startdate=enddate- relativedelta(months=1)

        else :
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        down_tse_kline(startdate,enddate)
        #parse_tse_kline(startdate,enddate)
    elif sys.argv[1]=='-t0' :
        """
        1. 大盤低位階,產生 轉折大紅K,或是強烈的莊家信號==>買進信號
        2. 策略選股比較==> 先寫單一股票
        3. 停損 停利模式比較==>mode1:持股60天 
        stock_no=sys.argv[2]  
        date=sys.argv[3] 回測日期,and stock buy signal generate
            
        """
        if len(sys.argv)==4 :
            #stock_no=sys.argv[2]
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            ##for loop 日期
            ## 根據 策略 產生 選股list
            ## 產生 order list 並算出 停損日  停利日
            ## 計算當日利損
            order_list=[]
            day=0
            nowdate=startdate
            profit=0.0
            result=[]
            stoplossmode=0
            tStart = time.time()
            while   nowdate<=enddate :
                if comm.check_work_date(nowdate)==True:
                    profit=0.0
                    fin_win=0
                    fin_lose=0
                    _win=0
                    _lose=0
                    day_result=[]
                    for order in order_list:        
                        sp=order.simulate(nowdate)  
                        #print(lno(),order.stock_id,nowdate,sp)
                        if order.action=='finish':
                            if order.profit_on_account>=1:
                                fin_win=fin_win+1
                            else:
                                fin_lose=fin_lose+1
                        else:
                            if order.profit_on_account>=1:
                                _win=_win+1
                            else:
                                _lose=_lose+1
                        profit=sp+profit
                    if len(order_list):    
                        #print(lno(),order.action)
                        print(lno(),'%d%02d%02d'%(nowdate.year,nowdate.month,nowdate.day),len(order_list),'%.3f'%(profit),fin_win,fin_lose,'%.3f'%(profit/len(order_list)))
                        day_result.append('%d-%02d-%02d'%(nowdate.year,nowdate.month,nowdate.day))
                        day_result.append(len(order_list))
                        day_result.append(fin_win)
                        day_result.append(fin_lose)
                        day_result.append(_win)
                        day_result.append(_lose)
                        day_result.append(profit)
                        day_result.append(profit/len(order_list))
                        result.append(day_result)
                    stock_df=good_stock.get_good_stock_m1(nowdate)
                    #print(lno(),stock_df)
                    if len(stock_df)!=0:
                        for i in stock_df['證券代號'].values.tolist():
                            if len(i)!=4:
                                continue
                            order_list.append(gen_buy_order(i,nowdate,stop_loss_mode=stoplossmode))    
                day=day+1    
                nowdate = startdate + relativedelta(days=day) 
            tEnd = time.time()
            print ("It cost %.3f sec" % (tEnd - tStart))    
            labels = ['date','下單次數','獲利單','賠錢單','帳上獲利單','帳上賠錢單','總獲利','平均獲利']
            df_save = pd.DataFrame.from_records(result, columns=labels)
            filen='out/backtest/%d%02d%02d-%d%02d%02d_mode%d.csv'%(startdate.year,startdate.month,startdate.day,enddate.year,enddate.month,enddate.day,stoplossmode)
            comm.check_dst_folder(os.path.dirname(filen))
            df_save.to_csv(filen,encoding='utf-8', index=False) 
    elif sys.argv[1]=='-t' :
        if len(sys.argv)==4 :
            #stock_no=sys.argv[2]
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            
            tStart = time.time()
            bt=backtest(startdate,enddate,sellmode=0,strategy=0)
            bt.start()
            bt.report()    
            tEnd = time.time()
    
            print ("It cost %.3f sec" % (tEnd - tStart))    
            """    
            labels = ['date','下單次數','獲利單','賠錢單','帳上獲利單','帳上賠錢單','總獲利','平均獲利']
            df_save = pd.DataFrame.from_records(result, columns=labels)
            filen='out/backtest/%d%02d%02d-%d%02d%02d_mode%d.csv'%(startdate.year,startdate.month,startdate.day,enddate.year,enddate.month,enddate.day,stoplossmode)
            comm.check_dst_folder(os.path.dirname(filen))
            df_save.to_csv(filen,encoding='utf-8', index=False)    
            """
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
    
