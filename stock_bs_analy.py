# -*- coding: utf-8 -*-
#import grs
import csv
import os
import sys
import time
import logging
from datetime import datetime
from datetime import timedelta
#from grs import Stock
#from stock_comm import OTCNo
#from stock_comm import TWSENo
import stock_comm as comm
import inspect
#import urllib2
import lxml.html
from bs4 import BeautifulSoup  
tdcc_file='data/tdcc_date.csv'
from inspect import currentframe, getframeinfo
from datetime import datetime
from dateutil.relativedelta import relativedelta
import inspect
import traceback
DEBUG=1
LOG=1
import logging
import requests
import pandas as pd
import numpy as np
import twii
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def ppp(string):
    if DEBUG:
        frame = inspect.currentframe()
        stack_trace = traceback.format_stack(frame)
        logging.debug(stack_trace[:-1])
    if LOG:
        logging.info(string)
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)
def check_dst_folder(dstpath):
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath)       
    return
def calc_bpwr(row):
    try :
        int(row['stock_id'])
    except:
        return 0
    try:
        open=float(row['open'])
        close=float(row['close'])
        diff=float(row['diff'])
    except:
        print (lno(),row)
        return 0

    high=float(row['high'])
    low=float(row['low'])
    vol=float(row['vol'])  
    prev_close=close-diff  
    if diff>0:
        buy1=abs(open-prev_close)
        sell1=abs(open-low)
        buy2=abs(high-low)
        sell2=abs(high-close)
    else:
        buy1=abs(high-open)
        sell1=abs(prev_close-open)
        buy2=abs(close-low)
        sell2=abs(high-low) 
    total_buy=buy1+buy2
    total_sell=sell1+sell2

    if total_buy+total_sell==0 :
        #print (lno(),total_buy,total_sell)
        if diff==0:
            return float(row['cash'])/2
        if diff >0:
            return float(row['cash'])
        print (lno(),open,high,low,close,diff)
        print (lno(),row)  
        return 0
    
    buy_pwr=float(row['cash'])*total_buy/(total_buy+total_sell)
    #buy_pwr=close*vol*total_buy/(total_buy+total_sell)
    #sell_pwr=vol*total_buy/(total_buy+total_sell)
    return buy_pwr
def calc_spwr(row):
    try :
        int(row['stock_id'])
    except:
        return 0
    try:
        open=float(row['open'])
        close=float(row['close'])
        diff=float(row['diff'])
    except:
        print (lno(),row)
        return 0
    try :
        #s_pwr=float(row['vol'])*float(row['close'])-row['b_pwr']
        s_pwr=float(row['cash'])-float(row['b_pwr'])
    except:
        print (lno(),row)
        pass
    return s_pwr
def calc_stock_bs(date,debug=1):
    out_list=[]
    df_tse_day=comm.get_tse_exchange_data(date,ver=1)
    if len(df_tse_day)==0:
        return []
    print(lno(),df_tse_day)
    #out_list.append(date.timestamp())    
    out_list.append(date.strftime("%Y-%m-%d"))    
    #out_list.append(np.datetime64(date))    
    df=df_tse_day
    df=df.replace('--',np.NaN)
    df=df.replace('---',np.NaN)
    df.dropna(inplace=True)    
    df['b_pwr'] = df.apply(calc_bpwr, axis=1)
    df['s_pwr'] = df.apply(calc_spwr, axis=1)
    total_buy=df['b_pwr'].sum()/100000000
    total_sell=df['s_pwr'].sum()/100000000
    out_list.append(total_buy)    
    out_list.append(total_sell)    
    #print(lno(),'tse',total_buy,total_sell)
    df_otc_day=comm.get_otc_exchange_data(date,ver=1)
    if len(df_otc_day)==0:
        return []
    df=df_otc_day
    df=df.replace('--',np.NaN)
    df=df.replace('---',np.NaN)
    df.dropna(inplace=True)    
    df['b_pwr'] = df.apply(calc_bpwr, axis=1)
    df['s_pwr'] = df.apply(calc_spwr, axis=1)
    total_buy=df['b_pwr'].sum()/100000000
    total_sell=df['s_pwr'].sum()/100000000
    out_list.append(total_buy)    
    out_list.append(total_sell)      
    #print(lno(),'otc',total_buy,total_sell)
    #print(lno(),out_list)
    return out_list
def gen_stock_bs_oneday(date,debug=1):
    out_file='csv/bs_tse_otc/bs_tse_otc_date.csv'
    dst_folder='csv/bs_tse_otc'
    check_dst_folder(dst_folder)

    _list=calc_stock_bs(date)
    if len(_list)==0:
        return 
    res=[]
    res.append(_list)
    print(lno(),res)
    labels = ['日期','tse_buy', 'tse_sell', 'otc_buy', 'otc_sell']
    res_df = pd.DataFrame.from_records(res, columns=labels)   
    print (lno(),res_df)    
    if os.path.exists(out_file): 
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print(lno(),df_s['date'].dtype)
        #print(lno(),res_df['date'].dtype)
        df_s=df_s.append(res_df,ignore_index=True)
        #print(lno(),df_s[['date','自營商total']])
        df_s.drop_duplicates(subset=['日期'],keep='first',inplace=True)
        #print(lno(),df_s[['date','外資total','投信total','自營商total']])
        df_s.to_csv(out_file,encoding='utf-8', index=False)
        
    else :
        res_df.to_csv(out_file,encoding='utf-8', index=False)
def gen_stock_bs(startdate,enddate):
    nowdatetime = enddate
    day=0
    while   nowdatetime>=startdate :
        gen_stock_bs_oneday(nowdatetime)
        day=day+1
        nowdatetime = enddate - relativedelta(days=day)   

def get_stock_bs_bydate(startdate,enddate,debug=1):
    out_file='csv/bs_tse_otc/bs_tse_otc_date.csv'
    if os.path.exists(out_file): 
        startdate_str='%d-%02d-%02d'%(int(startdate.year), int(startdate.month),int(startdate.day))
        enddate_str='%d-%02d-%02d'%(int(enddate.year), int(enddate.month),int(enddate.day))
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print(lno(),df_s[(df_s['date'] == date_str)].values.tolist())
        return df_s[(df_s['日期'] >= startdate_str)&(df_s['日期'] <= enddate_str)]
    else :
        return []         
if __name__ == '__main__':

    
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv)==1:
        startdate=datetime.today().date()
        gen_stock_bs_oneday(startdate)
  
             
    elif sys.argv[1]=='-g' :
        if len(sys.argv)==3 :
            #參數2:開始日期 
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            gen_stock_bs_oneday(startdate)
            #df['外資buy']=df['外資buy'].astype('float64')            
        if len(sys.argv)==4 :    
            
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            gen_stock_bs(startdate,enddate)

        else :
            print (lno(),'func -g date')          
    
    elif sys.argv[1]=='-t' :
        if len(sys.argv)==3 :
            #參數2:開始日期 
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            df=get_stock_bs_bydate(startdate,startdate)
            print(lno(),df)
        if len(sys.argv)==4 :    
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            df=get_stock_bs_bydate(startdate,enddate)    
            print(lno(),df)
        
    else:
        print (lno(),"unsport ")
        sys.exit()
    
