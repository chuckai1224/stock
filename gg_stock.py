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
import director
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
        try:
            d.at[0,'本益比']=df.iloc[0]['本益比']
        except:
            print(lno(),d.iloc[0]['stock_id'],'本益比',df.iloc[0]['本益比'])
            d.at[0,'本益比']=np.NaN    
        d.at[0,'股價淨值比']=df.iloc[0]['股價淨值比']
        d.at[0,'殖利率(%)']=df.iloc[0]['殖利率(%)']
        d.at[0,'股利年度']=df.iloc[0]['股利年度']
    else:
        d.at[0,'本益比']=np.NaN
        d.at[0,'股價淨值比']=np.NaN
        d.at[0,'殖利率(%)']=np.NaN
        d.at[0,'股利年度']=np.NaN
        return 'N'
    return 'Y'    
def get_stock_director(d,ver=1):
    if ver==1:
        df=director.get_mops_stock_director_df(d.iloc[0]) 
        for i in range(0,6):
            try:
                d.at[0,'前{}月董監持股'.format(i)]=df.iloc[i]['全體董監持股合計']/1000 #股數轉張數
            except:    
                d.at[0,'前{}月董監持股'.format(i)]=np.NaN
        d.at[0,'董監持股增減']= d.at[0,'前0月董監持股']- d.at[0,'前1月董監持股']    
        #print(lno(),df.head(6))
  
        if len(df)!=0:
            df['date']=df['date'].apply(time642str)
            d.at[0,'董監日期']=', '.join(map(str,df['date'].values.tolist()))
            d.at[0,'董監持股']=', '.join(map(str,df['全體董監持股合計'].values.tolist()))
        else:
            print(lno(),"get director NG")    
            print(lno(),d.iloc[0])    
        
        #raise    
        return
    
    try:
        df=comm.get_stock_director_df(d.iloc[0]) 
    except:
        print(lno(),d.iloc[0])
        return    
        #raise    
    for i in range(0,6):
        try:
            d.at[0,'前{}月董監持股'.format(i)]=df.iloc[-1-i]['董監持股']
        except:    
            d.at[0,'前{}月董監持股'.format(i)]=np.NaN
    d.at[0,'董監持股增減']= d.at[0,'前0月董監持股']- d.at[0,'前5月董監持股']         

def get_stock_tdcc_dist(d,debug=0):
    cols=['15','16','17','18','19','20','21','22','23','24','25','26','27','28','29']
    s_cols=['15','16','17','18','19','20','21','22','23','24','25']
    b_cols=['29']
    
    #     大戶近一周增加比,大戶近一月增加比,散戶近一月增加比
    #df.iloc[-1][cols].values.sum()
    df=get_stock_tdcc_dist_df(d.iloc[0]).tail(8).reset_index(drop=True) 
    #print(lno(),df)

    date_list=[]
    s_ratio_list=[]
    b_ratio_list=[]
    for i in range(0,len(df)):
        total_stock=df.iloc[-1-i][cols].values.sum()
        date=df.iloc[-1-i]['date']
        date_list.append(time642str(date))
        #print(lno(),date)
        #散戶比例
        s_ratio=df.iloc[-1-i][s_cols].values.sum()/total_stock*100
        s_ratio_list.append(s_ratio)
        #大戶比例
        b_ratio=df.iloc[-1-i]['29']/total_stock*100
        b_ratio_list.append(b_ratio)

    d.at[0,'股權日期']=  ', '.join(map(str,date_list))
    d.at[0,'大戶持股']=  ', '.join(map(str,b_ratio_list))
    d.at[0,'散戶持股']=  ', '.join(map(str,s_ratio_list))
          
    if len(df)>=4:
        """
        print(lno(),d.at[0,'前1周<400張比例'],d.at[0,'前4周<400張比例'])
        print(lno(),s_ratio_list[0],s_ratio_list[3])
        print(lno(),d.at[0,'前1周>1000張比例'],d.at[0,'前2周>1000張比例'],d.at[0,'前4周>1000張比例'])
        print(lno(),b_ratio_list[0],b_ratio_list[1],b_ratio_list[3])
        """
        d.at[0,'散戶近一月增加比']=(s_ratio_list[0]-s_ratio_list[3])/s_ratio_list[3]
        d.at[0,'大戶近一月增加比']=(b_ratio_list[0]-b_ratio_list[3])/b_ratio_list[3]
        d.at[0,'大戶近一周增加比']=(b_ratio_list[0]-b_ratio_list[1])/b_ratio_list[1]
        if debug==1:
            print(lno(),d.iloc[0])
        #raise
    else:
        d.at[0,'散戶近一月增加比']=np.NaN
        d.at[0,'大戶近一月增加比']=np.NaN
        d.at[0,'大戶近一周增加比']=np.NaN
def get_stock_industry_status(d):
    df=comm.get_stock_industry_status_df(d.iloc[0])  
    try:
        d.at[0,'產業']=df.iloc[0]['產業']
        d.at[0,'細產業']=df.iloc[0]['細產業']
        d.at[0,'產業地位']=df.iloc[0]['產業地位']
    except:
        print(lno(),'get_stock_industry_status error',d.iloc[0]    )
    #print(lno(),df)
    #raise       
def get_stock_revenue(d):
    df=comm.get_stock_revenue_df(d.iloc[0]) 
    if len(df):
        
        #print(lno(),df.iloc[0])
        try:
            d.at[0,'本年累計營收年增率']=float(df.iloc[0]['前期比較增減(%)'])
            d.at[0,'最新單月營收年增率']=float(df.iloc[0]['去年同月增減(%)'])
            d.at[0,'最新單月營收月增率']=float(df.iloc[0]['上月比較增減(%)'])
            d.at[0,'備註']=df.iloc[0]['備註']
        except:
            check_dst_folder('error')
            df.to_csv('error/{}_revenue.csv'.format(df.iloc[0]['公司代號']),encoding='utf-8', index=False,header=0)     
            
            print(lno(),df)
            #raise
    else:
        d.at[0,'本益比']=np.NaN
def get_stock_season_composite_income_sheet(d,debug=0):
    df=comm.get_stock_season_df(d.iloc[0],debug=0) 
    
    if len(df)==0:
        return 'N'
    ##get 第4季df 計算 前3年eps 營收收入
    d1=df[df['season']==4].reset_index(drop=True)
    
    years=len(d1)
    if years>3:
        years=3
    for i in range(0,years):
        if i==0:
            d.at[0,'本年EPS']=d1.iloc[i]['基本每股盈餘（元）']
        else:
            d.at[0,'前{}年EPS'.format(i)]=d1.iloc[i]['基本每股盈餘（元）']    
    for i in range(0,years):
        if i==0:
            d.at[0,'本年營收(百萬)']=float(d1.iloc[i]['營業收入']/1000 ) 
        else:
            d.at[0,'前{}年營收(百萬)'.format(i)]=float(d1.iloc[i]['營業收入']/1000 ) 
    ##計算3年psr high low and 計算psrS
    
    try:
        rev_S=(1+float(d.at[0,'本年累計營收年增率'])/100)*float(d1.iloc[0]['營業收入']/1000)
        d.at[0,'psrS']=d.at[0,'市值(百萬)']/rev_S
        d.at[0,'去年營收年增率']=(float(d1.iloc[1]['營業收入']/1000 ) /float(d1.iloc[2]['營業收入']/1000 ) -1)*100
    except:
        d.at[0,'去年營收年增率']=np.NaN
        d.at[0,'psrS']=np.NaN
        print(lno(),"get_stock_season_composite_income_sheet error  check ",df)
    #print(lno(),r_df.iloc[0])
    #raise

    stk=get_stock_data()
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
            d.at[0,'psr高-{}'.format(i+1)]=0  
            d.at[0,'psr低-{}'.format(i+1)]=0
    if len(psrlow)==3:    
        d.at[0,'psr三年最高']=max(psrhigh)
        d.at[0,'psr三年最低']=min(psrlow)
        d.at[0,'psrs/psr三年最低-1']=d.at[0,'psrS']/d.at[0,'psr三年最低']  -1
        d.at[0,'psrs/psr三年最高']=d.at[0,'psrS']/d.at[0,'psr三年最高']
    else:
        d.at[0,'psr三年最低']=0
        d.at[0,'psr三年最高']=0
        d.at[0,'psrs/psr三年最低-1']=0
        d.at[0,'psrs/psr三年最高']=0
    #d.d.at[0,'psrS']=np.NaN  
    
    #print(d.iloc[0])
    #raise
    ##抓取前8季 eps 毛利率 盈利率 淨益率  計算近一季 三率季成長 年成長          
    seasons=len(df)
    if seasons>8:
        seasons=8      
    for i in range(0,seasons):
        #d.at[0,'{}.{}QEPS'.format(df.iloc[i]['year'],df.iloc[i]['season'])]=df.iloc[i]['單季EPS']
        if i==0:
            d.at[0,'本季EPS']=df.iloc[i]['單季EPS']
        else:        
            d.at[0,'前{}季EPS'.format(i)]=df.iloc[i]['單季EPS']
    for i in range(0,seasons):
        print(lno(),df.iloc[i]['單季毛利淨額'],df.iloc[i]['單季營收'])
        #d.at[0,'{}.{}Q毛利率'.format(df.iloc[i]['year'],df.iloc[i]['season'])]=df.iloc[i]['單季毛利淨額']/df.iloc[i]['單季營收']*100
        if i==0:
            d.at[0,'本季毛利率']=df.iloc[i]['單季毛利淨額']/df.iloc[i]['單季營收']*100
        else:        
            d.at[0,'前{}季毛利率'.format(i)]=df.iloc[i]['單季毛利淨額']/df.iloc[i]['單季營收']*100
    for i in range(0,seasons):
        #d.at[0,'{}.{}Q營利率'.format(df.iloc[i]['year'],df.iloc[i]['season'])]=df.iloc[i]['單季營業利益淨額']/df.iloc[i]['單季營收']*100
        if i==0:
            d.at[0,'本季營利率']=df.iloc[i]['單季營業利益淨額']/df.iloc[i]['單季營收']*100
        else:        
            d.at[0,'前{}季營利率'.format(i)]=df.iloc[i]['單季營業利益淨額']/df.iloc[i]['單季營收']*100
    for i in range(0,seasons):
        #d.at[0,'{}.{}Q淨利率'.format(df.iloc[i]['year'],df.iloc[i]['season'])]=df.iloc[i]['單季綜合損益總額']/df.iloc[i]['單季營收']*100    
        if i==0:
            d.at[0,'本季淨利率']=df.iloc[i]['單季綜合損益總額']/df.iloc[i]['單季營收']*100    
        else:        
            d.at[0,'前{}季淨利率'.format(i)]=df.iloc[i]['單季綜合損益總額']/df.iloc[i]['單季營收']*100    
    try:
        d.at[0,'近一季毛利率升降(年)']=df.iloc[0]['單季毛利淨額']/df.iloc[0]['單季營收']*100 -df.iloc[4]['單季毛利淨額']/df.iloc[4]['單季營收']*100                
        d.at[0,'近一季營利率升降(年)']=df.iloc[0]['單季營業利益淨額']/df.iloc[0]['單季營收']*100 -df.iloc[4]['單季營業利益淨額']/df.iloc[4]['單季營收']*100                
        d.at[0,'近一季淨利率升降(年)']=df.iloc[0]['單季綜合損益總額']/df.iloc[0]['單季營收']*100 -df.iloc[4]['單季綜合損益總額']/df.iloc[4]['單季營收']*100                
        d.at[0,'近一季毛利率升降(季)']=df.iloc[0]['單季毛利淨額']/df.iloc[0]['單季營收']*100 -df.iloc[1]['單季毛利淨額']/df.iloc[1]['單季營收']*100                
        d.at[0,'近一季營利率升降(季)']=df.iloc[0]['單季營業利益淨額']/df.iloc[0]['單季營收']*100 -df.iloc[1]['單季營業利益淨額']/df.iloc[1]['單季營收']*100                
        d.at[0,'近一季淨利率升降(季)']=df.iloc[0]['單季綜合損益總額']/df.iloc[0]['單季營收']*100 -df.iloc[1]['單季綜合損益總額']/df.iloc[1]['單季營收']*100        
    except:
        pass
    if debug==1:
        print(lno(),d1)
       

def get_psrs_div_psr3y_score(r,debug=0):  
    if debug==1:
        print(lno(),r['psrs/psr三年最低-1'])
    x=r['psrs/psr三年最低-1']
    if 0.2-x >2:
        y=2
    elif 0.2-x <-2 :
        y=-2
    else:
        y=(0.2-x)*2
    if debug==1:    
        print(lno(),y)    
    return y

def get_networth_score(r,debug=0):  
    if debug==1:
        print(lno(),r['股價淨值比'])
    x=r['股價淨值比']
    if x>=3.3: 
        y=-2
    elif x>=1.3:
        y=1.3-x
    else:
        y=(1.3-x)*2
    if debug==1:    
        print(lno(),y)   
    return y
def get_psrs_score(r):  
    print(lno(),r['psrS'])
    psrs=r['psrS']
    if psrs>=3:
        y=-4
    else:
        y=(1.5-psrs)/0.375
    print(lno(),y)   
    return y

def get_prr_score(r):  
    print(lno(),r['prr'])
    prr=r['prr']
    if prr>=15:
        y=-2
    elif prr<=0:
        y=-2    
    else :
        y=(15-prr)/7.5
    print(lno(),y)  
    #raise  
    return y
def get_Gross_margin_score(r,debug=0):
    if debug==1:
        print(lno(),r['本季毛利率'])
    x=r['本季毛利率']
    if x<10:
        y=-2
    elif x>=60:    
        y=2
    else:
        y=(x-30)/15  
    if debug==1:    
        print(lno(),y)  
    #raise  
    return y
def get_Operating_Profit_margin_score(r,debug=0):
    if debug==1:
        print(lno(),r['本季營利率'])
    try:
        x=r['本季營利率']-r['前4季營利率']
    except:
        return np.NaN    
    if x/2.5<-2:
        y=-2
    elif x/2.5>2:
        y=2
    else:
        y=x/2.5    
    #raise  
    return y
def get_revenue_year_20_score(r,debug=0):
    if debug==1:
        print(lno(),'最新單月營收年增率',r['最新單月營收年增率'])
        print(lno(),'本年累計營收年增率',r['本年累計營收年增率'])
        print(lno(),'去年營收年增率',r['去年營收年增率'])
    x=r['最新單月營收年增率']/100
    w=r['本年累計營收年增率']/100-r['去年營收年增率']/100
    if (x*100-10)*0.25/10+w*0.25>2:
        y=2
    else:
        y=(x*100-10)*0.25/10+w*0.25
    #raise  
    return y
def get_revenue_month_80_score(r,debug=0):
    if debug==1:
        print(lno(),'最新單月營收月增率',r['最新單月營收月增率'])
    x=r['最新單月營收月增率']/100
    if x>0.8:
        y=2
    elif x<-0.8:
        y=-2    
    else:
        y=  x*2.5  
    #raise  
    return y
def get_peg_score(n,debug=0):
    r=n.copy()
    try:
        r['今年eps預估']=r['本季EPS']*2+r['前1季EPS']+r['前2季EPS']
        r['去年eps']=r['前4季EPS']*2+r['前5季EPS']+r['前6季EPS']
        a=r['今年eps預估']/r['去年eps'] -1
    except:
        return np.NaN    
    if a<0 and r['去年eps']<0:
        a=0-a
    r['eps預估年成長率']=a
    
    if r['eps預估年成長率']<0:
        peg=1.34
    else:
        if  r['今年eps預估']<0:
            peg=1.34
        else:
            peg=r['收盤價']/ r['今年eps預估']/r['eps預估年成長率']/100
    """
    if 本益比=NA
        if 今年eps預估<0
            peg=1.34
        else
            peg=股價/今年eps預估/eps預估年成長率/100
    else        
        peg=本益比/(eps預估年成長率)/100
    """    
    if peg>1.34:
        y=-2
    elif peg <0.66:
        y=2
    else:
        y=(1-peg)*2/0.34  
    #raise 
    if debug!=0: 
        print(lno(),r[['今年eps預估','本季EPS','前1季EPS','前2季EPS']])
        print(lno(),r[['去年eps','前4季EPS','前5季EPS','前6季EPS']])
        print(lno(),r['eps預估年成長率'],peg,y)
    return y

def time642str(x):
        ts = pd.to_datetime(str(x)) 
        d = ts.strftime('%y-%m-%d')
        return d
    
def gen_stock_info(r,debug=0):
    #cols=['date','stock_id']
    cols=[ 'stock_id', 'stock_name','總分', '董監持股增減','大戶近一月增加比','大戶近一周增加比','散戶近一月增加比',
       'psrS','prr', '本益比', '股價淨值比', '殖利率(%)',  '收盤價', '股數(萬張)', '市值(百萬)',
       '產業地位', '備註','細產業','產業',
       '本年累計營收年增率', '最新單月營收年增率','最新單月營收月增率', 
       #'前0月董監持股', '前1月董監持股', '前2月董監持股', '前3月董監持股', '前4月董監持股', '前5月董監持股',
       #'前1周>1000張比例', '前2周>1000張比例','前3周>1000張比例', '前4周>1000張比例', 
       #'前1周<400張比例', '前2周<400張比例', '前3周<400張比例', '前4周<400張比例',
       'market',
       '股利年度', 
       '本年EPS', '前1年EPS', '前2年EPS', '本年營收(百萬)',
       '前1年營收(百萬)', '前2年營收(百萬)', '去年營收年增率', 'psr高-1', 'psr低-1',
       'psr高-2', 'psr低-2', 'psr高-3', 'psr低-3', 'psr三年最高', 'psr三年最低',
       'psrs/psr三年最低-1', 'psrs/psr三年最高', '本季EPS', '前1季EPS', '前2季EPS', '前3季EPS',
       '前4季EPS', '前5季EPS', '前6季EPS', '前7季EPS', '本季毛利率', '前1季毛利率', '前2季毛利率',
       '前3季毛利率', '前4季毛利率', '前5季毛利率', '前6季毛利率', '前7季毛利率', '本季營利率', '前1季營利率',
       '前2季營利率', '前3季營利率', '前4季營利率', '前5季營利率', '前6季營利率', '前7季營利率', '本季淨利率',
       '前1季淨利率', '前2季淨利率', '前3季淨利率', '前4季淨利率', '前5季淨利率', '前6季淨利率', '前7季淨利率',
       '近一季毛利率升降(年)', '近一季營利率升降(年)', '近一季淨利率升降(年)', '近一季毛利率升降(季)',
       '近一季營利率升降(季)', '近一季淨利率升降(季)',  '分數:psrs/psr(3)-1', '分數:淨值比',
       '分數:psrs', '分數:prr', '分數:毛利率', '分數:營利率年增', '分數:營收年增20%', '分數:營收月增80%',
       '分數:peg','date' ,
       'week kline open','week kline high','week kline low', 'week kline close', 'week kline date',
       'day kline open', 'day kline high', 'day kline low', 'day kline close', 'day kline date',
       'week kline vol','day kline vol',
       '股權日期','大戶持股','散戶持股',
       '董監日期','董監持股','big3 date','外資','投信','自營商'
       ]
    stock_id=r.stock_id
    
    date=r.date
    tse_stock=comm.exchange_data('tse').get_df_date_parse(date)['stock_id'].tolist()     
    d=pd.DataFrame(np.empty(( 1, len(cols))) * np.nan, columns = cols)
    str_col_list=[
        'stock_id','stock_name','market','產業地位', '備註','細產業','產業','董監日期','董監持股',
        'big3 date','外資','投信','自營商',
        '股權日期','大戶持股','散戶持股',
        'week kline open','week kline high','week kline low', 'week kline close', 'week kline date',
        'day kline open', 'day kline high', 'day kline low', 'day kline close', 'day kline date',
        'week kline vol','day kline vol']
    for i in str_col_list:
        d[i]=d[i].astype('str')

    d.at[0,'date']=date
    d.at[0,'stock_id']=stock_id
    d.at[0,'stock_name']=r.stock_name
    if stock_id in tse_stock:
        d.at[0,'market']='tse'
    else:
        d.at[0,'market']='otc'  
    total_stock_nums=get_total_stock_num(stock_id,date)

    d.at[0,'收盤價']=get_stock_last_close(stock_id,date) 
    d.at[0,'股數(萬張)']=get_total_stock_num(stock_id,date)/10000000
    #1萬張=1千萬股 市值百萬 要x10    
    d.at[0,'市值(百萬)']=d.at[0,'收盤價']*d.at[0,'股數(萬張)']*10

    ## 抓取 董監持股
    get_stock_director(d)

    ##抓取 前1周<400張比例 ,前2周<400張比例,前3周<400張比例,前4周<400張比例
    ##     前1周>1000張比例 ,前2周>1000張比例,前3周>1000張比例,前4周>1000張比例
    ##     大戶近一周增加比,大戶近一月增加比,散戶近一月增加比
    get_stock_tdcc_dist(d)

    ##抓取 本益比 淨值比 殖利率 股利年度
    res=get_stock_pe_networth_yield(d)
    if res=='N':
        return pd.DataFrame()

    ## 抓取 產業地位
    get_stock_industry_status(d)
    
    ##抓取 	本年累計營收年增率	最新單月營收年增率	最新單月營收月增率	
    get_stock_revenue(d)

    ##三年eps 營業收入 psr high low psrS  8季三率 近一季三率成長 去年營收年增率 去年營收-百萬	今年營收預估-百萬
    res=get_stock_season_composite_income_sheet(d)  
    if res=='N':
        return pd.DataFrame()

    #get prr
    d.at[0,'prr']=get_stock_prr(d.iloc[0])   
    print(lno(),d.columns)
    d.at[0,'分數:psrs/psr(3)-1']= get_psrs_div_psr3y_score(d.iloc[0])   
    d.at[0,'分數:淨值比']= get_networth_score(d.iloc[0])   
    d.at[0,'分數:psrs']= get_psrs_score(d.iloc[0])   
    d.at[0,'分數:prr']= get_prr_score(d.iloc[0])   
    d.at[0,'分數:毛利率']= get_Gross_margin_score(d.iloc[0])   
    d.at[0,'分數:營利率年增']= get_Operating_Profit_margin_score(d.iloc[0])   
    d.at[0,'分數:營收年增20%']= get_revenue_year_20_score(d.iloc[0]) 
    d.at[0,'分數:營收月增80%']= get_revenue_month_80_score(d.iloc[0]) 
    d.at[0,'分數:peg']= get_peg_score(d.iloc[0]) 
    
    
    score_cols=['分數:psrs/psr(3)-1','分數:淨值比','分數:psrs','分數:prr','分數:毛利率','分數:營利率年增','分數:營收年增20%','分數:營收月增80%','分數:peg']
    if debug==1:
        print(lno(),d.iloc[0][score_cols].values)
    d.at[0,'總分']= sum(d.iloc[0][score_cols].values)
    df1=comm.get_stock_df_bydate_nums(stock_id,300,date)
    df1['vol']=df1['vol']/1000
    #week_df=kline.resample(df1,'W',60).reset_index(drop=True).copy()
    week_df=kline.resample(df1,'W-FRI',60).reset_index(drop=True).copy()
    def date2str(x):
        return datetime.strftime(x,'%y-%m-%d')
    week_df['date']=week_df['date'].apply(date2str)
    if debug!=0:
        print(lno(),week_df.iloc[0])
        print(lno(),len(week_df))
  
    if len(week_df)>=60:
        d.at[0,'week kline open']=', '.join(map(str,week_df['open'].values.tolist()))
        d.at[0,'week kline high']=', '.join(map(str,week_df['high'].values.tolist()))
        d.at[0,'week kline low']=', '.join(map(str,week_df['low'].values.tolist()))
        d.at[0,'week kline close']=', '.join(map(str,week_df['close'].values.tolist()))
        d.at[0,'week kline date']=', '.join(week_df['date'].values.tolist())
        d.at[0,'week kline vol']=', '.join(map(str,week_df['vol'].values.tolist()))
    else:    
        d.at[0,'week kline open']=''
        d.at[0,'week kline high']=''
        d.at[0,'week kline low']=''
        d.at[0,'week kline close']=''
        d.at[0,'week kline date']=''
        d.at[0,'week kline vol']=' '
    day_df=df1.tail(60).reset_index(drop=True).copy()
    #print(lno(),day_df)
    """
    d['day kline open']=d['day kline open'].astype(str)
    d['day kline high']=d['day kline high'].astype(str)
    d['day kline low']=d['day kline low'].astype(str)
    d['day kline close']=d['day kline close'].astype(str)
    d['day kline date']=d['day kline date'].astype(str)
    d['day kline vol']=d['day kline vol'].astype(str)
    """
    day_df['date']=day_df['date'].apply(time642str)
    d.at[0,'day kline open']=', '.join(map(str,day_df['open'].values.tolist()))
    d.at[0,'day kline high']=', '.join(map(str,day_df['high'].values.tolist()))
    d.at[0,'day kline low']=', '.join(map(str,day_df['low'].values.tolist()))
    d.at[0,'day kline close']=', '.join(map(str,day_df['close'].values.tolist()))
    d.at[0,'day kline date']=', '.join(day_df['date'].values.tolist())
    d.at[0,'day kline vol']=', '.join(map(str,day_df['vol'].values.tolist()))
    stock_3big_df=stock_big3.get_stock_3big(stock_id,date,10,d.at[0,'market'])
    
    if len(stock_3big_df)!=0:
        stock_3big_df['date']=stock_3big_df['日期'].apply(time642str)
        d.at[0,'big3 date']=', '.join(stock_3big_df['date'].values.tolist())
        d.at[0,'外資']=', '.join(map(str,stock_3big_df['外資'].values.tolist()))
        d.at[0,'投信']=', '.join(map(str,stock_3big_df['投信'].values.tolist()))
        d.at[0,'自營商']=', '.join(map(str,stock_3big_df['自營商'].values.tolist()))
        print(lno(),d.iloc[0][['外資','投信']])
    else:    
        d.at[0,'big3 date']=''
        d.at[0,'外資']=' '
        d.at[0,'投信']=' '
        d.at[0,'自營商']=' '
    #print(lno(),d.columns)
    #raise
    d=d[cols]
    return d
def check_skip_stock(r):
    close=np.nan
    if len(r.stock_id)!=4:
        return np.NaN 
    if r['stock_id'].startswith('00'):
        return np.NaN 
    if r['stock_id'].startswith( '25' ):
        return np.NaN 
    if r['stock_id'].startswith( '28' ):
        return np.NaN 
    if r['stock_id'].startswith( '55' ):
        return np.NaN 
    if r['stock_id'].startswith( '58' ):
        return np.NaN 
    cash=float(r.cash)
    if cash<3000000:
        return np.nan
    return r.close  
def red_K_ratio_calc(r):
    try:
        pre_close=r.close-r['diff']
        if r.open >pre_close:
            red_K_pwr= (r.close -pre_close)/pre_close
        else :
            red_K_pwr= (r.close -r.open)/pre_close    
        return red_K_pwr 
    except :
        return np.nan
def check_point_K(r):
    stk=get_stock_data()
    df1=stk.get_df_by_enddate_num(r.stock_id,r.date,122)
    #print(lno(),df1)
    if len(df1)<90:
        return 0
    df1['red_K_ratio']=df1.apply(red_K_ratio_calc, axis=1)
    top10=df1.sort_values(by='red_K_ratio',ascending=False).iloc[10]['red_K_ratio']
    #print(lno(),df1,top10)
    if df1.iloc[-1]['red_K_ratio']>=top10:
        return 1
    return   0
def gen_pointK_list(date):
    df=comm.get_tse_otc_stock_df_by_date(date)
    df['close']=df.apply(check_skip_stock,axis=1)
    df = df[~df['close'].isnull()]
    df['point_K']=df.apply(check_point_K,axis=1)
    d=df[(df['point_K']>=1)].copy()
    return d    

def gen_fund_ratio_list(date):
    df=stock_big3.get_stock_big3_date_df(date) 
    def check_fund_skip_stock(r):
        if len(r['證券代號'])!=4:
            return 0 
        if r['證券代號'].startswith('00'):
            return 0
        
        if r['投信買賣超股數']>0:
            return 1
        return 0    
    df['result']=df.apply(check_fund_skip_stock,axis=1)
    df=df[df['result']==1].reset_index(drop=True)
    d1=df[['證券代號','證券名稱','投信買賣超股數','date','market']].copy()
    d1.columns=['stock_id','stock_name','投信買賣超股數','date','market']
    #print(lno(),d1)
    return d1
    
def gen_gg_buy_list(date,rev_date,method):
    
    #,'stock_name','market','收盤價','股數(萬張)','市值(百萬)']
    if 'revenue'==method:
        d1=revenue.gen_revenue_good_list(rev_date)
    elif 'pointK'==method:
        d1=gen_pointK_list(date)
    elif 'fund'==method:
        d1=gen_fund_ratio_list(date)    
    else:
        d1=director.gen_director_good_list(rev_date) 
    #print(lno(),d1)
    if len(d1)==0:
        return
    d1['date']=date
    d1[['收盤價','股本','市值']]=d1.apply(get_market_value,axis=1,result_type="expand")
    ##TODO 50億 check
    if 'fund'!=method:
        d1=d1[d1['市值']<=15000].reset_index(drop=True)
    out=pd.DataFrame()
    for i in range(0,len(d1)):
    #for i in range(0,1):
        if d1.iloc[i]['stock_id'].startswith( '25' ):
            continue
        if d1.iloc[i]['stock_id'].startswith( '28' ):
            continue
        if d1.iloc[i]['stock_id'].startswith( '55' ):
            continue
        if d1.iloc[i]['stock_id'].startswith( '58' ):
            continue
        ## TODO BYPASS TEST
        #if d1.loc[i,'stock_id']!='4979':
        #    continue
        d=gen_stock_info(d1.iloc[i])
        if len(d)==0:
            continue
        if 'fund'==method: 
            d.at[0,'投本比']=d1.iloc[i]['投信買賣超股數']/(d1.iloc[i]['股本']*100000)
            print(lno(),d1.iloc[i])
            print(lno(),d.iloc[0])
        if len(out)==0 :
            out=d.copy()
        else:
            out=out.append(d,ignore_index=True)  
    if 'fund'!=method:        
        out=out.sort_values(by=['總分'], ascending=False).copy()
    else:
        out=out.sort_values(by=['投本比'], ascending=False).copy()    
        c = out.pop('投本比')             
        out.insert(16,'投本比',c)  
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)
    ##=IMPORThtml("https://raw.githubusercontent.com/chuckai1224/final/master/fut_day_report_fin.html","table",1)
    out.to_html('final/{}_good.html'.format(method),escape=False,index=False,sparsify=True,border=2,index_names=False)
    pd.set_option('display.max_colwidth', old_width)  
    out.to_csv('final/{}_good_{}.csv'.format(method,date.strftime('%Y%m%d')),encoding='utf-8', index=False)
    
g_stk = None
def get_stock_data():
    global g_stk
    if g_stk==None:
        print(lno(),'111')
        g_stk=comm.stock_data()
    return g_stk
    
import tdcc_dist
g_tdcc=None
def get_tdcc_dist():
    global g_tdcc
    if g_tdcc==None:
        g_tdcc=tdcc_dist.tdcc_dist()
    return g_tdcc 

def get_total_stock_num(stock_id,date):
    tdcc=get_tdcc_dist() 
    total_stock_nums=tdcc.get_total_stock_num(stock_id,date)
    return total_stock_nums
def get_stock_last_close(stock_id,date):
    stk=get_stock_data()    
    df=stk.get_df_by_startdate_enddate(stock_id,date-relativedelta(days=14),date+relativedelta(days=1))  
    if len(df.index)==0:
        return np.NaN
    return df.iloc[-1]['close']
def get_stock_tdcc_dist_df(r):
    tdcc=get_tdcc_dist() 
    df=tdcc.get_df(r.stock_id)
    return df    
def get_market_value(r):
    #return 百萬
    date=r.date
    stock_id=r.stock_id
    total_stock_nums=get_total_stock_num(stock_id,date)
    #print(lno(),r.stock_id,total_stock_nums)
    if total_stock_nums==0:
        return 
    last_close=get_stock_last_close(stock_id,date)
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
        nowdate=comm.get_date() 
        rev_date=nowdate-relativedelta(months=1)
        gen_gg_buy_list(nowdate,rev_date,"pointK")                            
        gen_gg_buy_list(nowdate,rev_date,"revenue")                      
        gen_gg_buy_list(nowdate,rev_date,"director")  
        gen_gg_buy_list(nowdate,rev_date,"fund")                   
    elif sys.argv[1]=='gg' :
        ## TODO gg gen_analy_data
        try:    
            nowdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        except:
            nowdate=comm.get_date()  
        try:    
            rev_date=datetime.strptime(sys.argv[3],'%Y%m%d')
        except:
            rev_date=nowdate-relativedelta(months=1)
        gen_gg_buy_list(nowdate,rev_date,"pointK")                            
        gen_gg_buy_list(nowdate,rev_date,"revenue")                      
        gen_gg_buy_list(nowdate,rev_date,"director")    
        gen_gg_buy_list(nowdate,rev_date,"fund")   
    elif sys.argv[1]=='point' :
        ## TODO gg gen_analy_data
        try:    
            nowdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        except:
            nowdate=comm.get_date()  
        try:    
            rev_date=datetime.strptime(sys.argv[3],'%Y%m%d')
        except:
            rev_date=nowdate-relativedelta(months=1)
        gen_gg_buy_list(nowdate,rev_date,"pointK")       
    elif sys.argv[1]=='director' :
        ## TODO gg gen_analy_data
        try:    
            nowdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        except:
            nowdate=comm.get_date()  
        try:    
            rev_date=datetime.strptime(sys.argv[3],'%Y%m%d')
        except:
            rev_date=nowdate-relativedelta(months=1)
        gen_gg_buy_list(nowdate,rev_date,"director")    
    
    elif sys.argv[1]=='fund' :
        ## TODO gg gen_analy_data
        try:    
            nowdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        except:
            nowdate=comm.get_date()  
        try:    
            rev_date=datetime.strptime(sys.argv[3],'%Y%m%d')
        except:
            rev_date=nowdate-relativedelta(months=1)
        gen_gg_buy_list(nowdate,rev_date,"fund")  
    elif sys.argv[1]=='revenue' :
        ## TODO gg gen_analy_data
        try:    
            nowdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        except:
            nowdate=comm.get_date()  
        try:    
            rev_date=datetime.strptime(sys.argv[3],'%Y%m%d')
        except:
            rev_date=nowdate-relativedelta(months=1)
        gen_gg_buy_list(nowdate,rev_date,"revenue")                          
    else:
        pass    
    