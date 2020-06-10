# -*- coding: utf-8 -*-

import csv
import os
import sys
import time
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import requests
from io import StringIO
from inspect import currentframe, getframeinfo
from sqlalchemy import create_engine
import stock_comm as comm
import inspect
import traceback
DEBUG=1
LOG=1
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)
def check_dst_folder(dstpath):
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath)     
def check(r):
    try:
        #print(lno(),r[0],type(r[0]))
        #print(lno(),r)
        if len(r['stock_id'])!=4:
            return 0
        if r['stock_id'].startswith('00'):
            return 0
        return 1
    except:
        #print(lno(),r[0],type(r[0]))
        return 0
      
        
       
    
def parse_stock_director_xq(startdate,enddate):
    nowdate=startdate
    while   nowdate<=enddate :
        
        _csv='data/director/xq/director{}.csv'.format(nowdate.strftime('%Y%m'))
        if os.path.exists(_csv):
            dfs = pd.read_csv(_csv,encoding = 'big5hkscs',skiprows=5,header=None)
            try:
                #dfs.columns=['序號','stock_id','stock_name','成交','漲幅%','總量','董監持股佔股本比例']
                dfs.columns=['序號','stock_id','stock_name','成交','漲幅%','總量','d1','d2','董監持股','董監持股佔股本比例','符合條件數']
            except:
                print(lno(),dfs.iloc[0])
                dfs.columns=['序號','stock_id','stock_name','成交','漲幅%','總量','董監持股','董監持股佔股本比例','符合條件數']
                #raise
            #print(lno(),dfs.iloc[0])
            #raise    
            d=dfs[['stock_id','董監持股','董監持股佔股本比例']].copy()
            d['date']=datetime(nowdate.year,nowdate.month,15)
            for i in range(0,len(dfs)):
                print(lno(),d.iloc[i])
                #raise
                stock_id=d.iloc[i]['stock_id'].replace('.TW','')
                comm.stock_read_sql_add_df(stock_id,'director',d[i:i+1])
        else:
             print(lno(),_csv)   
        nowdate = nowdate + relativedelta(months=1)   
def parse_xq_rr(year,season):

    date1=datetime(year,season*3,1)+relativedelta(months=1)
    date1=date1-relativedelta(days=1)
    while True:
        d1=comm.exchange_data('tse').get_df_date_parse(date1)
        if len(d1)==0:
            date1=date1-relativedelta(days=1)
            continue
        else:
            d2=comm.exchange_data('otc').get_df_date_parse(date1)
            d3=pd.concat([d1,d2])
            
            break
    d3['check_stock_id']=d3.apply(check,axis=1)
    d3=d3[d3['check_stock_id']==1].reset_index(drop=True)

    d_ref=d3[['stock_id','date','stock_name']].copy()    
    
    print(lno(),d_ref.columns)    
    _csv='xq_data/rr{}.{}Q.csv'.format(year,season)
    if os.path.exists(_csv):
        print(lno(),_csv)
        dfs = pd.read_csv(_csv,encoding = 'big5hkscs',skiprows=5,header=None)
        try:
            #dfs.columns=['序號','stock_id','stock_name','成交','漲幅%','總量','董監持股佔股本比例']
            #序號,	代碼,	商品,	成交,	漲幅%,	總量,	研發費用(百萬),	符合條件數
            dfs.columns=['序號','stock_id','stock_name','成交','漲幅%','總量','研發費用(百萬)','符合條件數']
        except:
            print(lno(),dfs.iloc[0])
            dfs.columns=['序號','stock_id','stock_name','成交','漲幅%','總量','收盤價','區間漲幅','研發費用(百萬)','符合條件數']
            #raise
        def rmTW(r):
            return r['stock_id'].replace('.TW','')
        dfs['stock_id']=dfs.apply(rmTW,axis=1)    
        d=dfs[['stock_id','研發費用(百萬)']].copy()
        df_o=pd.merge(d_ref,d,how='left').reset_index(drop=True)
        df_o['YQ']='{}.{}'.format(year,season)
        print(lno(),df_o)
        df_o.to_html('test.html',escape=False,index=False,sparsify=True,border=2,index_names=False)
        for i in range(0,len(df_o)):
            print(lno(),df_o.iloc[i])
            #raise
            stock_id=df_o.iloc[i]['stock_id']
            comm.stock_read_sql_add_df(stock_id,'RD_fee',df_o[i:i+1])
        #print(lno(),d.iloc[0])
        return df_o
    print(lno(),_csv)
    return pd.DataFrame()
        
        
def gen_director_good_list(date,debug=0):
    nowdate=date
    cnt=0
    while cnt<=3:
        _csv='data/director/xq/director{}.csv'.format(nowdate.strftime('%Y%m'))
        if  os.path.exists(_csv):
            break
        nowdate=nowdate - relativedelta(months=1)
        cnt=cnt+1
    d=get_xq_month_df(nowdate)
    if len(d):
        prev_month = nowdate - relativedelta(months=1)
        d_prev=get_xq_month_df(prev_month)
        if len(d_prev):
            d_prev.columns=['stock_id','stock_name','前1月董監持股','前1月董監持股佔股本比例']
            df_out=pd.merge(d,d_prev)
            def calc_director_add(r):
                try:
                    add= float(r['董監持股'])-float(r['前1月董監持股'])
                except:
                    print(lno(),r) 
                    raise   
                return add        
            df_out['董監持股增減']=df_out.apply(calc_director_add,axis=1)
            df_good=df_out[df_out['董監持股增減']>100].copy().reset_index(drop=True)
            def removetw(r):
                return r['stock_id'].replace('.TW','')    
            df_good['stock_id']=df_good.apply(removetw,axis=1)
            
            return df_good
    else:
        nowdate=nowdate - relativedelta(months=1)
        cnt+=1
    return pd.DataFrame()        
           
    
        
        
                     
if __name__ == '__main__':

    if len(sys.argv)==1:
        startdate=datetime.today().date()
        
        startdate=datetime(2019,8,1)
        enddate=datetime(2020,3,1)
        parse_stock_director_xq(startdate,enddate)
    elif sys.argv[1]=='rr' :
        year =int(sys.argv[2])
        season =int(sys.argv[3])
        parse_xq_rr(year,season)
            
    
         
    elif sys.argv[1]=='gen' :
        if len(sys.argv)==3 :
            #參數2:開始日期 
            datatime=datetime.strptime(sys.argv[2],'%Y%m%d')
            gen_revenue_final_file(datatime)
        else :
            print (lno(),'func -g date')
    elif sys.argv[1]=='get' :
        if len(sys.argv)==4 :
            #參數2:開始日期 
            stock_id=sys.argv[2]
            datatime=datetime.strptime(sys.argv[3],'%Y%m%d')
            #get_revenue_by_stockid_bydate(stock_id,datatime)
            #get_revenue_by_stockid(stock_id,datatime)
            df=sql_data.get_by_date(datatime)
            print(lno(),df)
    elif sys.argv[1]=='sql' :
        
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        try:
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        except:
            enddate=startdate
        now_date = startdate 
        while   now_date<=enddate :
            #down_tse_monthly_report(int(now_date.year),int(now_date.month))
            #down_otc_monthly_report(int(now_date.year),int(now_date.month))
            sql_data.download(now_date)
            #gen_revenue_final_file(now_date)
            now_date = now_date + relativedelta(months=1)
   
        
    else:
        print (lno(),"unsport ")
        sys.exit()
    
    