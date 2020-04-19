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
import inspect
import traceback
import stock_comm as comm
DEBUG=1
LOG=1
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)
def check_dst_folder(dstpath):
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath)     
  
def down_pe_networth_yield(date,dw=1,debug=1):
    dst_folder='data/down_pe_networth_yield'
    check_dst_folder(dst_folder)
    
    filename='%s/%s.html'%(dst_folder,date.strftime('%Y%m%d'))
    # 偽瀏覽器
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    tseurl='http://www.twse.com.tw/exchangeReport/BWIBBU_d?response=csv&date={}&selectType=ALL'.format(date.strftime('%Y%m%d'))
    #url='https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera.php?l=zh-tw&o=csv&se=EW&t=D&d=%d/%02d/%02d&s=0,asc,0'%(int(nowdatetime.year)-1911,int(nowdatetime.month),int(nowdatetime.day))
    otcurl='https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php?l=zh-tw&o=csv&d=%d/%02d/%02d&c=&s=0,asc,0'%(date.year-1911,date.month,date.day)
    url_list=[tseurl,otcurl]
    #url_list=[otcurl]
    
    for url in url_list:
        if 'twse' in url:
            filename='%s/tse%s.html'%(dst_folder,date.strftime('%Y%m%d'))
            ofile='%s/tse%s.csv'%(dst_folder,date.strftime('%Y%m%d'))
            skip_rows=1
            columns=['stock_id', '證券名稱', '殖利率(%)', '股利年度', '本益比', '股價淨值比', '財報年/季','dummy']
            
        else:
            filename='%s/otc%s.html'%(dst_folder,date.strftime('%Y%m%d'))
            ofile='%s/otc%s.csv'%(dst_folder,date.strftime('%Y%m%d'))
            columns=['stock_id', '證券名稱', '本益比', '每股股利', '股利年度', '殖利率(%)', '股價淨值比']
            skip_rows=3
        #if os.path.exists(filename):
        #    return 
        if dw==1 :
            print(lno(),url)
            r = requests.get(url)
            if not r.ok:
                print(lno(),"Can not get data at {}".format(url))
                return pd.DataFrame() 
            with open(filename, 'wb') as file:
                # A chunk of 128 bytes
                for chunk in r:
                    file.write(chunk)
            r.close()        
            #time.sleep(5)
        if os.path.getsize(filename)<4096:
            print(lno(),filename,"size:",os.path.getsize(filename));
            os.remove(filename)    
        if not os.path.exists(filename): 
            return pd.DataFrame()
        try:    
            dfs = pd.read_csv(filename,encoding = 'big5hkscs',skiprows=skip_rows)
            dfs=dfs.dropna(thresh=2)
        except:
            print(lno(),filename,"ng file")
            return pd.DataFrame()
        dfs.columns=columns
        d=dfs[['stock_id','本益比', '股價淨值比', '殖利率(%)','股利年度']].copy()
        d.to_csv(ofile,encoding='utf-8', index=False) 
        """ too long to save to sql
        for i in range(0,len(d)):
            comm.stock_df_to_sql(d.iloc[i]['stock_id'],'pe_ratio',d[i:i+1])
        """    
        print(lno(), d)
        
        if debug==1:
            #print(lno(),dfs)
            pass
            #print(lno(),len(dfs),dfs)
            #print(lno(),dfs[1].iloc[0][0] )
    #raise    
        

if __name__ == '__main__':

    if len(sys.argv)==1:
        now_date=datetime.today().date()
        
        down_pe_networth_yield(now_date) #new
        #down_tse_monthly_report(int(startdate.year),int(startdate.month)-1)
        #down_otc_monthly_report(int(startdate.year),int(startdate.month)-1)
        
    elif sys.argv[1]=='-d' :
        #print (lno(),len(sys.argv))
        
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        try:
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        except:
            enddate=startdate    
        now_date = startdate 
        while   now_date<=enddate :
            down_pe_networth_yield(now_date,dw=1) #new
            now_date = now_date + relativedelta(days=1)
   
        
    else:
        print (lno(),"unsport ")
        sys.exit()
    
    