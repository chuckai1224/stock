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
import stock_comm as cmm 
import requests
import inspect
from inspect import currentframe, getframeinfo
import pandas as pd
#import pyecharts
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

 
                
def down_taiwan_dollar(startdate,enddate):

    dst_folder='csv/taiwan_dollar'
    filename='csv/taiwan_dollar/tmp.csv'
    out_file='csv/taiwan_dollar/taiwan_dollar_data.csv'
    check_dst_folder(dst_folder)
    startdate_str='%d/%02d/%02d'%(int(startdate.year), int(startdate.month),int(startdate.day))
    enddate_str='%d/%02d/%02d'%(int(enddate.year), int(enddate.month),int(enddate.day))
    url = 'https://www.taifex.com.tw/cht/3/dailyFXRateDown'
    query_params = {
        'queryStartDate': startdate_str,
        'queryEndDate': enddate_str
    }

    page = requests.post(url, data=query_params)

    if not page.ok:
        print(lno(),"Can not get data at {}".format(url))
        return 
    with open(filename, 'wb') as file:
        # A chunk of 128 bytes
        for chunk in page:
            file.write(chunk)
    #print(lno(),filename)        
    df = pd.read_csv(filename,encoding = 'big5hkscs')
    df.dropna(axis=1,how='all',inplace=True)
    df.dropna(inplace=True)
    #print (lno(),df)
    if os.path.exists(out_file): 
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        
        
        df_s=df_s.append(df,ignore_index=True)
        
        df_s.drop_duplicates(subset=['日期'],keep='first',inplace=True)
        df_s=df_s.sort_values(by=['日期'], ascending=False)
        print(lno(),df_s.head())
        df_s.to_csv(out_file,encoding='utf-8', index=False)
        
    else :
        df.to_csv(out_file,encoding='utf-8', index=False)
    
   
def get_taiwan_dollor(date):
    out_file='csv/taiwan_dollar/taiwan_dollar_data.csv'
    #print(lno(),date)
    if os.path.exists(out_file): 
        
        date_str='%d/%02d/%02d'%(int(date.year), int(date.month),int(date.day))
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print(lno(),df_s[(df_s['date'] == date_str)].values.tolist())
        df=df_s[(df_s['日期'] == date_str)]
        if len(df)==1:
            df.reset_index(inplace=True)
            #total=float(df.iat[0,'外資total'].strip().replace(',', ''))
            try:
                total=float(df.at[0,'美元／新台幣'])
            except:
                print (lno(),df.at[0,'美元／新台幣'])  
                total=0        
            
            #print(lno(),total_int)
            return total
        return 0
        
    else :
        return 0         
     
        
if __name__ == '__main__':
    #print (lno(),sys.path[0])
    #get_cur_twii_list(datetime.today())
    if len(sys.argv)==1:
        startdate=stock_comm.get_date()
        down_taiwan_dollar(startdate,startdate)
    elif sys.argv[1]=='-d' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            # 從今日往前抓一個月
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            down_taiwan_dollar(startdate,enddate)  
        elif len(sys.argv)==3 :
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            down_taiwan_dollar(startdate,startdate)      
        else :
              print (lno(),'func -p startdata enddate') 
    elif sys.argv[1]=='-g' :
        if len(sys.argv)==3 :
            #參數2:開始日期 
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            dollar =get_taiwan_dollor(startdate)
            #df['外資buy']=df['外資buy'].astype('float64')            
            
            print(dollar)
            

        else :
            print (lno(),'func -g date')  
        
    
            
            
    else:   
        objdatetime=datetime.strptime(sys.argv[1],'%Y%m%d')
        show_twii(objdatetime)
        