# -*- coding: utf-8 -*-
#from __future__ import unicode_literals
import io
import csv
import os
import time
import timeit
import sys
#import urllib2
from datetime import datetime
from dateutil.relativedelta import relativedelta
try:
    import multiprocessing 
    from pandarallel import pandarallel
    #pandarallel.initialize()
    #from multiprocessing import Pool
    #from pathos.multiprocessing import ProcessingPool as Pool
    #n_processes = 4  # My machine has 4 CPUs
    #pool = Pool(processes=n_processes)
    pass
except:
    pass    
import stock_comm as comm 
import stock_big3
import tdcc_dist
import requests
import inspect
from inspect import currentframe, getframeinfo
import pandas as pd
import numpy as np
import op
import math
import kline
from sqlalchemy import create_engine
#from pyecharts import Kline
#from pyecharts import Candlestick
#import webbrowser
import revenue
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from time import sleep
from selenium.webdriver.support.ui import Select
import shutil
import talib
from talib import abstract
import scipy.signal as signal 
#from stocktool import comm as cm1 
import platform
import all_stock
import seaborn as sns
import matplotlib as mpl
import eps
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)

def check_dst_folder(dstpath):
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath)     


                
def get_gg_income_ratio(r):
    ratio1=np.nan
    result=np.nan
    if r.stock_id.startswith('00'):
        return 0
    if len(r.stock_id)!=4:
        return 0
    income=get_sql_income()
    #資料日的前一個月
    date=r.date-relativedelta(months=1)
    #當有輸入比較資料月
    if r.rev_month!=0:
        year=date.year
        if r.rev_month >date.month:
            year=year-1
        month=r.rev_month
        date=datetime(year,month,1)

    df =income.get_by_stockid_date_months(r.stock_id,date,4,debug=1) 
    if len(df)<2:
        print(lno(),r.stock_id)
        return 0
    if r.rev_month!=0 and df.iloc[-1]['date'].month!=r.rev_month:
        print(lno(),df.iloc[-1]['date'].year,df.iloc[-1]['date'].month) 
        return 0
    
    if df.iloc[-1]['去年同月增減(%)']<20:
        return 0
    if df.iloc[-2]['去年同月增減(%)']>20:
        return 0
    return df.iloc[-1]['去年同月增減(%)']
    #print(lno(),df[['date','公司代號','公司名稱','去年同月增減(%)']])
    #print(lno(),get_market_value(r))
##get 董監事持股 6個月內的增減    
def get_director_change(r):
    df=comm.get_director_df(r.stock_id,dw=0)
    d=df.head(6).copy()
    d['全體董監持股增減']=d['全體董監持股增減'].astype('float64')
    
    #print(lno(),d)
    #print(lno(),d['全體董監持股增減'].sum())
    return d['全體董監持股增減'].sum()

#市值/營收比 找<0.75  避開>1.5 絕對不買>3 
def get_stock_psr(r):
    df=comm.get_stock_last_year_income(r.date,r.stock_id)
    if len(df)!=0:
        ##百萬
        income= df.iloc[0]['營業收入']/1000
        market_value=r['市值']
        #print(lno(),r)
        ratio=float(r['累計年增率'])
        income1=income*(100+ratio)/100
        #print(lno(),income,income1)
        #print(lno(),df.iloc[0])
        #print(lno(),income,market_value)
        return market_value/income1
    return np.NaN 
#市值/研究發展比
def get_stock_prr(r):
    RD_fee=comm.get_stock_RD_fee(r.stock_id,dw=0)
    if RD_fee!=np.NaN:
        market_value=r['市值']
        #print(lno(),df.iloc[0])
        #print(lno(),income,market_value)
        return market_value/RD_fee
    return np.NaN 
          
def gen_gg_buy_list(date,rev_date):
    
    d1=revenue.gen_revenue_good_list(rev_date)
    
    d1['date']=date
    #print(lno(),d1)
    d1['市值']=d1.apply(get_market_value,axis=1)
    d1=d1[d1['市值']<=3000].reset_index(drop=True)
    
    stock_id='3228'
    ##get
   

    d1['董監事持股增減']=d1.apply(get_director_change,axis=1)
    #d1['psr']=d1.apply(get_stock_psr,axis=1)
    #d1['prr']=d1.apply(get_stock_prr,axis=1)
    print(lno(),d1)
    #d1.to_csv('./test.csv',encoding='utf-8', index=False)
    
    
        
def get_market_value(r):
    #return 百萬
    date=r.date
    stock_id=r.stock_id
    tdcc=get_tdcc_dist() 
    total_stock_nums=tdcc.get_total_stock_num(stock_id,date)
    #print(lno(),r.stock_id,total_stock_nums)
    if total_stock_nums==0:
        return 
    stk=get_stock_data()    
    df=stk.get_df_by_startdate_enddate(stock_id,date-relativedelta(days=7),date+relativedelta(days=1))   
     
    if len(df.index)==0:
        return 
    #print(lno(),r.stock_id,total_stock_nums)        
    #print(lno(),df)
    try:
        market_value=df.iloc[-1]['close']* total_stock_nums /1000000
    except:
        print(lno(),r)
        #print(lno(),df.iloc[0]['close'],total_stock_nums)
        #df=stk.get_df_by_startdate_enddate(stock_id,date-relativedelta(days=7),date+relativedelta(days=1))  
        #print(lno(),df)
        return
        raise        
    return  market_value
   
from sqlalchemy import create_engine       
from sqlalchemy.types import NVARCHAR, Float, Integer 

multitask=0
def df_apply_fun(df,func):
    global multitask
    if platform.system().upper()=='LINUX': 
        multitask=1   
        pandarallel.initialize()
        return df.parallel_apply(func,axis=1)
    else:
        multitask=0   
        return df.apply(func, axis=1)      
def pool_map(function_name, df, processes = multiprocessing.cpu_count()):
    if platform.system().upper()=='LINUX':
        n_processes = processes  # My machine has 4 CPUs
    else:
        n_processes = 1
    df_split = np.array_split(df, n_processes)
    return multiprocessing.Pool(processes).map(function_name, df_split)
g_stk = None
g_engine=None
g_con=None
def init_sql():
    global g_stk,g_engine,g_con
    if g_stk==None:
        print(lno())
        g_stk=comm.stock_data()
    if g_engine==None:
        g_engine = create_engine('sqlite:///sql/buy_signal.db', echo=False)  
    if g_con==None: 
        g_con = g_engine.connect()    
    return   g_stk,g_engine,g_con
 
def get_stock_data():
    global g_stk
    if multitask==1:
        stk=comm.stock_data()
    else:
        if g_stk==None:
            print(lno(),'111')
            g_stk=comm.stock_data()
        stk=g_stk
    return stk

g_tdcc=None
def get_tdcc_dist():
    global g_tdcc
    if multitask==1:
        tdcc=tdcc_dist.tdcc_dist()
        return tdcc
    else:
        if g_tdcc==None:
            g_tdcc=tdcc_dist.tdcc_dist()
        return g_tdcc 
g_sb3=None
def get_stock_big3():
    global g_sb3
    if multitask==1:
        sb3=stock_big3.stock_big3()
        return sb3
    else:
        if g_sb3==None:
            g_sb3=stock_big3.stock_big3()
            print(lno(),g_sb3)    
        return g_sb3 
g_income=None
def get_sql_income():
    global g_income
    if multitask==1:
        in1=revenue.income()
        return in1
    else:
        if g_income==None:
            g_income=revenue.income()
            print(lno(),g_income)    
        return g_income 
def mv5_vol_ratio(r):
    stk=get_stock_data()
    df1=stk.get_df_by_enddate_num(r.stock_id,r.date,20)
    try :
        df1['MV5'] = talib.MA(df1.vol,5)
        v_ratio=df1.iloc[-1]['vol']/df1.iloc[-2]['MV5']
    except:
        print(lno(),r.stock_id,r.date,"some thing wrong",df1)
        #raise
        return np.nan     
    return v_ratio
def upper_shadow(r):
    stk=get_stock_data()   
    df1=stk.get_df_by_enddate_num(r.stock_id,r.date,1)
    #print(lno(),r.date,df1)
    try:
        upper_shadow_val=df1.at[0,'high']-df1.at[0,'close']
        real_body=abs(df1.at[0,'close']-df1.at[0,'open'])
        if real_body==0:
            return np.nan
        return upper_shadow_val/real_body
    except:
        return np.nan    
def over_prev_high(r):
    stk=get_stock_data() 
    df1=stk.get_df_by_enddate_num(r.stock_id,r.date,20)
    #print(lno(),r.date,df1)
    try :
        over_phigh=df1.iloc[-1]['close']-df1.iloc[-2]['high']
    except:
        print(lno(),"some thing wrong",df1)
        return np.nan     
    return over_phigh
def ma_tangled_day(r):
    stk=get_stock_data() 
    df1=stk.get_df_by_enddate_num(r.stock_id,r.date-relativedelta(days=1),120)
    try :
        ma_list = [5,10,20]
        for ma in ma_list:
            df1['MA_' + str(ma)] = talib.MA(df1.close,ma)
    except:
        print(lno(),r.stock_id,r.date,len(df1.index))  
        if len(df1.index)==0:
            return np.nan      
        raise
    def calc_sizeway(r):
        max_value=max(r.MA_5,r.MA_10,r.MA_20)
        min_value=min(r.MA_5,r.MA_10,r.MA_20)
        if min_value==0:
            return np.nan
        return (max_value-min_value)/min_value*100
    sideway=df1.apply(calc_sizeway,axis=1)
    rev_sideway=sideway[::-1]
    cnt=0
    for i in rev_sideway:
        if i<=0.5:
            cnt=cnt+1
            continue
        break
    if abs(cnt)>=5 :
        #print(lno(),r.stock_id,r.date,tmp)    
        #kline.show_stock_kline_pic(r.stock_id,r.date,120)
        pass
    return cnt
     
  

def gen_out_stock_df():
    df = pd.read_csv('test.csv',encoding = 'utf-8')
    """
    籌碼面
    董監半年增加
    大戶周增加比率 散戶周增加比率
    大戶4周增加比率 散戶4周增加比率
    基本面
    
    """
    d=df[['stock_id','stock_name','date','market','市值']]
    print(lno(),d)
    pass


if __name__ == '__main__':
    sns.set()
    mpl.rcParams['font.family'] = 'sans-serif'
    mpl.rcParams[u'font.sans-serif'] = ['simhei']
    
    if len(sys.argv)==1:
        startdate=datetime(2019,12,13)  
        #gen_buy_list_step1(startdate)  
        gen_out_stock_df()
                           
    elif sys.argv[1]=='gg' :
        ## TODO gg gen_analy_data
        try:    
            nowdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        except:
            nowdate=stock_comm.get_date()  
        try:    
            rev_date=datetime.strptime(sys.argv[3],'%Y%m%d')
        except:
            rev_date=nowdate-relativedelta(months=1)
              
        gen_gg_buy_list(nowdate,rev_date)                      
    else:
        pass    
    