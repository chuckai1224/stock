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
def get_stock_psrS(r):
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
def get_stock_prr(r,debug=1):
    RD_fee=comm.get_stock_RD_fee(r.stock_id,dw=0)
    if debug==1:
        print(lno(),RD_fee)
    if RD_fee==0:
        return 0
    if RD_fee!=np.NaN:
        market_value=r['市值(百萬)']
        if debug==1:
            print(lno(),r.stock_id)
            print(lno(),RD_fee,market_value)
        return market_value/RD_fee
    return np.NaN 
  

def get_stock_market(r):
    d1=comm.exchange_data('tse').get_df_date_parse(r.date)           
    if r.stock_id in d1['stock_id'].tolist():
        return 'tse'
    return 'otc'
##get 本益比 淨值比 殖利率 股利年度
def get_stock_pe_networth_yield(d):
    df=comm.get_stock_pe_networth_yield_df(d.iloc[0]) 
    if len(df):
        d.at[0,'本益比']=df.iloc[0]['本益比']
        d.at[0,'股價淨值比']=df.iloc[0]['股價淨值比']
        d.at[0,'殖利率(%)']=df.iloc[0]['殖利率(%)']
        d.at[0,'股利年度']=df.iloc[0]['股利年度']
    else:
        d.at[0,'本益比']=np.NaN
def get_stock_tdcc_dist(d):
    cols=['15','16','17','18','19','20','21','22','23','24','25','26','27','28','29']
    s_cols=['15','16','17','18','19','20','21','22','23','24','25']
    b_cols=['29']
    
    #     大戶近一周增加比,大戶近一月增加比,散戶近一月增加比
    #df.iloc[-1][cols].values.sum()
    df=comm.get_stock_tdcc_dist_df(d.iloc[0]) 
    if len(df)>=4:
        print(lno(),d.iloc[0])
        for i in range(0,4):
            total_stock=df.iloc[-1-i][cols].values.sum()
            d.at[0,'前{}周<400張比例'.format(i+1)]=df.iloc[-1-i][s_cols].values.sum()/df.iloc[-1-i][cols].values.sum()
            d.at[0,'前{}周>1000張比例'.format(i+1)]=df.iloc[-1-i]['29']/df.iloc[-1-i][cols].values.sum()
        d.at[0,'散戶近一月增加比']=(d.at[0,'前1周<400張比例']-d.at[0,'前4周<400張比例'])/d.at[0,'前4周<400張比例']*100
        d.at[0,'大戶近一月增加比']=(d.at[0,'前1周>1000張比例']-d.at[0,'前4周>1000張比例'])/d.at[0,'前4周>1000張比例']*100
        d.at[0,'大戶近一周增加比']=(d.at[0,'前1周>1000張比例']-d.at[0,'前2周>1000張比例'])/d.at[0,'前2周>1000張比例']*100
        print(lno(),d.iloc[0])
        raise
    else:
        d.at[0,'本益比']=np.NaN
def get_stock_revenue(d):
    df=comm.get_stock_revenue_df(d.iloc[0]) 
    if len(df):
        
        #print(lno(),df.iloc[0])
        d.at[0,'本年累計營收年增率']=float(df.iloc[0]['前期比較增減(%)'])
        d.at[0,'最新單月營收年增率']=float(df.iloc[0]['去年同月增減(%)'])
        d.at[0,'最新單月營收月增率']=float(df.iloc[0]['上月比較增減(%)'])
        d.at[0,'備註']=df.iloc[0]['備註']
        #print(lno(),d.iloc[0])
        #raise
    else:
        d.at[0,'本益比']=np.NaN
def get_stock_season_composite_income_sheet(d):
    df=comm.get_stock_season_df(d.iloc[0]) 
    ##get 第4季df 計算 前3年eps 營收收入
    d1=df[df['season']==4].reset_index(drop=True)
    years=len(d1)
    if years>3:
        years=3
    for i in range(0,years):
        d.at[0,'{}EPS'.format(d1.iloc[i]['year'])]=d1.iloc[i]['基本每股盈餘（元）']
    for i in range(0,years):
        d.at[0,'{}營收(百萬)'.format(d1.iloc[i]['year'])]=float(d1.iloc[i]['營業收入']/1000 ) 
    ##計算3年psr high low and 計算psrS
    rev_S=(1+float(d.at[0,'本年累計營收年增率'])/100)*float(d1.iloc[0]['營業收入']/1000)
    d.at[0,'psrS']=d.at[0,'市值(百萬)']/rev_S
    #print(lno(),r_df.iloc[0])
    #raise

    stk=comm.get_stock_data()
    psrhigh=[]
    psrlow=[]
    for i in range(0,years):    
        startdate=datetime(d1.iloc[i]['year'],1,1)
        enddate=datetime(d1.iloc[i]['year'],12,31)
        ds=stk.get_df_by_startdate_enddate(d.iloc[0]['stock_id'],startdate,enddate) 
        if len(ds):
            #print(lno(),ds.iloc[0])
            high=max(ds['high'])
            low=min(ds['low'])
            #營業收入
            psrhigh.append(d.at[0,'股數(萬張)']*10000 *high/d1.iloc[i]['營業收入'])
            
            psrlow.append(d.at[0,'股數(萬張)']*10000 *low/d1.iloc[i]['營業收入'])
            d.at[0,'psr高-{}'.format(i+1)]=d.at[0,'股數(萬張)']*10000 *high/d1.iloc[i]['營業收入']    
            d.at[0,'psr低-{}'.format(i+1)]=d.at[0,'股數(萬張)']*10000 *low/d1.iloc[i]['營業收入']
            #print(lno(),high,low,d1.iloc[i]['year'])
        else:
            d.at[0,'psr高-{}'.format(i+1)]=np.NaN    
            d.at[0,'psr低-{}'.format(i+1)]=np.NaN
    if len(psrlow)==3:    
        d.at[0,'psr三年最高']=max(psrhigh)
        d.at[0,'psr三年最低']=min(psrlow)
        d.at[0,'psrs/psr三年最低-1']=d.at[0,'psrS']/d.at[0,'psr三年最低']  -1
        d.at[0,'psrs/psr三年最高']=d.at[0,'psrS']/d.at[0,'psr三年最高']
    else:
        d.at[0,'psr三年最低']=np.NaN
        d.at[0,'psr三年最高']=np.NaN
        d.at[0,'psrs/psr三年最低-1']=np.NaN
        d.at[0,'psrs/psr三年最高']=np.NaN
    #d.d.at[0,'psrS']=np.NaN  
    
    #print(d.iloc[0])
    #raise
    ##抓取前8季 eps 毛利率 盈利率 淨益率  計算近一季 三率季成長 年成長          
    seasons=len(df)
    if seasons>8:
        seasons=8      
    for i in range(0,seasons):
        d.at[0,'{}.{}QEPS'.format(df.iloc[i]['year'],df.iloc[i]['season'])]=df.iloc[i]['單季EPS']
    for i in range(0,seasons):
        d.at[0,'{}.{}Q毛利率'.format(df.iloc[i]['year'],df.iloc[i]['season'])]=df.iloc[i]['單季毛利淨額']/df.iloc[i]['單季營收']*100
    for i in range(0,seasons):
        d.at[0,'{}.{}Q營利率'.format(df.iloc[i]['year'],df.iloc[i]['season'])]=df.iloc[i]['單季營業利益淨額']/df.iloc[i]['單季營收']*100
    for i in range(0,seasons):
        d.at[0,'{}.{}Q淨利率'.format(df.iloc[i]['year'],df.iloc[i]['season'])]=df.iloc[i]['單季綜合損益總額']/df.iloc[i]['單季營收']*100    
    try:
        d.at[0,'近一季毛利率升降(年)']=df.iloc[0]['單季毛利淨額']/df.iloc[0]['單季營收']*100 -df.iloc[4]['單季毛利淨額']/df.iloc[4]['單季營收']*100                
        d.at[0,'近一季營利率升降(年)']=df.iloc[0]['單季營業利益淨額']/df.iloc[0]['單季營收']*100 -df.iloc[4]['單季營業利益淨額']/df.iloc[4]['單季營收']*100                
        d.at[0,'近一季淨利率升降(年)']=df.iloc[0]['單季綜合損益總額']/df.iloc[0]['單季營收']*100 -df.iloc[4]['單季綜合損益總額']/df.iloc[4]['單季營收']*100                
    except:
        pass
    d.at[0,'近一季毛利率升降(季)']=df.iloc[0]['單季毛利淨額']/df.iloc[0]['單季營收']*100 -df.iloc[1]['單季毛利淨額']/df.iloc[1]['單季營收']*100                
    d.at[0,'近一季營利率升降(季)']=df.iloc[0]['單季營業利益淨額']/df.iloc[0]['單季營收']*100 -df.iloc[1]['單季營業利益淨額']/df.iloc[1]['單季營收']*100                
    d.at[0,'近一季淨利率升降(季)']=df.iloc[0]['單季綜合損益總額']/df.iloc[0]['單季營收']*100 -df.iloc[1]['單季綜合損益總額']/df.iloc[1]['單季營收']*100        
    print(lno(),d1)    
def gen_stock_info(r):
    cols=['date','stock_id']
    stock_id=r.stock_id
    date=r.date
    tse_stock=comm.exchange_data('tse').get_df_date_parse(date)['stock_id'].tolist()     
    d=pd.DataFrame(np.empty(( 1, len(cols))) * np.nan, columns = cols)
    d['stock_id']=d['stock_id'].astype('str')
    d.at[0,'date']=date
    d.at[0,'stock_id']=stock_id
    d.at[0,'stock_name']=r.stock_name
    if stock_id in tse_stock:
        d.at[0,'market']='tse'
    else:
        d.at[0,'market']='otc'  
    total_stock_nums=comm.get_total_stock_num(stock_id,date)

    d.at[0,'收盤價']=comm.get_stock_last_close(stock_id,date) 
    d.at[0,'股數(萬張)']=comm.get_total_stock_num(stock_id,date)/10000000
    #1萬張=1千萬股 市值百萬 要x10    
    d.at[0,'市值(百萬)']=d.at[0,'收盤價']*d.at[0,'股數(萬張)']*10
    """
    抓取 前1周<400張比例 ,前2周<400張比例,前3周<400張比例,前4周<400張比例
         前1周>1000張比例 ,前2周>1000張比例,前3周>1000張比例,前4周>1000張比例
         大戶近一周增加比,大戶近一月增加比,散戶近一月增加比
    """
    get_stock_tdcc_dist(d)
    ##抓取 本益比 淨值比 殖利率 股利年度
    #get_stock_pe_networth_yield(d)
    ##抓取 	本年累計營收年增率	最新單月營收年增率	最新單月營收月增率	
    get_stock_revenue(d)
    ##三年eps 營業收入 psr high low psrS  8季三率 近一季三率成長 去年營收年增率 去年營收-百萬	今年營收預估-百萬
    get_stock_season_composite_income_sheet(d)  
    #get prr
    d.at[0,'prr']=get_stock_prr(d.iloc[0])    
    print(lno(),d.iloc[0])
def gen_gg_buy_list(date,rev_date):
    
    #,'stock_name','market','收盤價','股數(萬張)','市值(百萬)']
    d1=revenue.gen_revenue_good_list(rev_date)
    d1['date']=date
    d1[['收盤價','股本','市值']]=d1.apply(get_market_value,axis=1,result_type="expand")
    d1=d1[d1['市值']<=3000].reset_index(drop=True)
  
    #d1['market']=d1.apply(get_stock_market,axis=1)
     
    for i in range(0,len(d1)):
        gen_stock_info(d1.iloc[i])
        
        raise
    print(lno(),d.iloc[0])

    raise
    
    
    ##get
   

    d1['董監事持股增減']=d1.apply(get_director_change,axis=1)
    #d1['psr']=d1.apply(get_stock_psr,axis=1)
    #d1['prr']=d1.apply(get_stock_prr,axis=1)
    #PEG
    #大戶近一周增加比率
    #近8季毛利率
    #近8季營利率
    #淨值
    #股價淨值比
    
    print(lno(),d1.iloc[0])
    print(lno(),d1.iloc[1])
    #d1.to_csv('./test.csv',encoding='utf-8', index=False)
    
    
        
def get_market_value(r):
    #return 百萬
    date=r.date
    stock_id=r.stock_id
    total_stock_nums=comm.get_total_stock_num(stock_id,date)
    #print(lno(),r.stock_id,total_stock_nums)
    if total_stock_nums==0:
        return 
    last_close=comm.get_stock_last_close(stock_id,date)
    if last_close==np.NaN:
        return 
    #print(lno(),r.stock_id,total_stock_nums)        
    #print(lno(),df)
    try:
        market_value=last_close* total_stock_nums /1000000
    except:
        print(lno(),r)
        #print(lno(),df.iloc[0]['close'],total_stock_nums)
        #df=stk.get_df_by_startdate_enddate(stock_id,date-relativedelta(days=7),date+relativedelta(days=1))  
        #print(lno(),df)
        return
        raise        
    return last_close,total_stock_nums/10000000, market_value
   
 


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
    