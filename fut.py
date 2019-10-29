# -*- coding: utf-8 -*-
#from __future__ import unicode_literals
import io
import csv
import os
import time
import sys
#import urllib2
from datetime import datetime
from dateutil.relativedelta import relativedelta
#from grs import RealtimeWeight
import stock_comm
import stock_comm as comm 
import requests
import inspect
from inspect import currentframe, getframeinfo
import pandas as pd
import numpy as np
import op
#from pyecharts import Kline
#from pyecharts import Candlestick
#import webbrowser
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)

def check_dst_folder(dstpath):
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath)     


                
def down_data(enddate,download=1):

    dst_folder='data/fut'
    filename='data/fut/tmp.csv'
    out_file='data/fut/%d%02d%02d.csv'%(int(enddate.year), int(enddate.month),int(enddate.day))
    check_dst_folder(dst_folder)
    enddate_str='%d/%02d/%02d'%(int(enddate.year), int(enddate.month),int(enddate.day))
    url = 'https://www.taifex.com.tw/cht/3/futDailyMarketReport'


    query_params = {
        'queryType': '2',
        'marketCode': '0',
        'dateaddcnt':'',
        'commodity_id':'TX',
        'commodity_id2':'',
        'queryDate': enddate_str,
        'MarketCode':'0','commodity_idt':'TX','commodity_id2t':'','commodity_id2t2':''
        
    }
    #print(lno(),download)
    if download==1:
        #print(lno(),url,query_params)
        page = requests.post(url, data=query_params)

        if not page.ok:
            print(lno(),"Can not get data at {}".format(url))
            return 
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in page:
                file.write(chunk)
    if not os.path.exists(filename): 
        return
    
    dfs = pd.read_html(filename,encoding = 'utf8')
    #print(lno(),len(dfs))
    for df in dfs :
        #print(lno(),df.iloc[0].values.tolist())
        if '契約' in df.iloc[0].values.tolist():
            columns=df.iloc[0].values.tolist()
            df.columns=columns
            
            df.dropna(axis=1,how='all',inplace=True)
            df.dropna(inplace=True)
            df=df.drop([0]).reset_index(drop=True)
            df.to_csv(out_file,encoding='utf-8', index=False)
            

def generate_final(enddate):
    dst_folder='data/fut/final'
    check_dst_folder(dst_folder)
    out_file='data/fut/final/fut.csv'
    add_file='data/fut/%d%02d%02d.csv'%(int(enddate.year), int(enddate.month),int(enddate.day))
    
    if os.path.exists(add_file): 
        dfs = pd.read_csv(add_file,encoding = 'utf-8',dtype= {'公司代號':str})
        dfs.dropna(axis=1,how='all',inplace=True)
        dfs.dropna(inplace=True)
        #dfs.columns=['公司代號','公司名稱','本季eps','本期綜合損益總額','毛利率','營利率','純益率']
        df=dfs.head(1).copy()
        str_date='%d/%02d/%02d'%(int(enddate.year), int(enddate.month),int(enddate.day))
        df['日期']=str_date
        df['日期']=[comm.date_sub2time64(x) for x in df['日期'] ]  
        df=df[['日期','最後成交價']]
        print(lno(),df)
        if os.path.exists(out_file): 
            df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype= {'日期':str})
            df_s['日期']=[comm.date_sub2time64(x) for x in df_s['日期'] ]  
            df_s=df_s.append(df,ignore_index=True)
            df_s.dropna(axis=1,how='all',inplace=True)
            df_s.dropna(inplace=True)
            df_s.drop_duplicates(subset=['日期'],keep='last',inplace=True)
            df_s=df_s.sort_values(by=['日期'], ascending=False)
            #print(lno(),df_s)
            df_s.to_csv(out_file,encoding='utf-8', index=False)
        
        else :
            df.to_csv(out_file,encoding='utf-8', index=False)
        
def down_gen_datas(startdate,enddate):
    #startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
    day=0
    nowdate=enddate
    while   nowdate>=startdate :
        nowdate = enddate - relativedelta(days=day)
        down_data(nowdate) 
        generate_final(nowdate) 
        day=day+1   
def get_df_bydate(date,debug=0):
    out_file='data/fut/final/fut.csv'
    #print(lno(),date)
    
    outcols=['日期','最後成交價']
    if os.path.exists(out_file): 
        date_str='%d-%02d-%02d'%(int(date.year), int(date.month),int(date.day))
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print(lno(),df_s[(df_s['date'] == date_str)].values.tolist())
        df=df_s[(df_s['日期'] == date_str)]
        if len(df)==1:
            df['日期']=[comm.date_sub2time64(x) for x in df['日期'] ]  
            df.reset_index(inplace=True)
            if debug==1:
                print(lno(),df)
            return df
        
        return pd.DataFrame(pd.np.empty(( 1, len(outcols))) * pd.np.nan, columns = outcols)
        
    else :
        return pd.DataFrame(pd.np.empty(( 1, len(outcols))) * pd.np.nan, columns = outcols)

def get_dfs_bydate(startdate,enddate,debug=0):
    out_file='data/fut/final/fut.csv'
    #print(lno(),date)
    outcols=['日期','最後成交價']
    if os.path.exists(out_file): 
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        df_s['日期']=[comm.date_sub2time64(x) for x in df_s['日期'] ]  
        #print(lno(),df_s[(df_s['date'] == date_str)].values.tolist())
        df=df_s[(df_s.loc[:,"日期"] <= np.datetime64(enddate))&(df_s.loc[:,"日期"] >= np.datetime64(startdate))].copy()
        
        if len(df)!=0:
            df=df.reset_index(drop=True)
            if debug==1:
                print(lno(),df)
            return df
        
        return pd.DataFrame(pd.np.empty(( 1, len(outcols))) * pd.np.nan, columns = outcols)
        
    else :
        return pd.DataFrame(pd.np.empty(( 1, len(outcols))) * pd.np.nan, columns = outcols)

 
if __name__ == '__main__':
    #print (lno(),sys.path[0])
    #get_cur_twii_list(datetime.today())
    if len(sys.argv)==1:
        startdate=stock_comm.get_date()
        down_fut_op_big3(startdate)
    elif sys.argv[1]=='-d' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==3 :
            # 從今日往前抓一個月
            enddate=datetime.strptime(sys.argv[2],'%Y%m%d')
            down_data(enddate)  
            generate_final(enddate)  
        elif len(sys.argv)==4 :
            # 從今日往前抓一個月
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            down_gen_datas(startdate,enddate)      
        else :
              print (lno(),'func -p startdata enddate') 
    elif sys.argv[1]=='-g' :
        if len(sys.argv)==3 :
            #參數2:開始日期 
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            list =get_df_bydate(startdate)
            #df['外資buy']=df['外資buy'].astype('float64')            
            print(list)
        else :
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            df =get_dfs_bydate(startdate,enddate)
            print (lno(),df)  
    else:   
        objdatetime=datetime.strptime(sys.argv[1],'%Y%m%d')
        #down_fut_op_big3(objdatetime)
        