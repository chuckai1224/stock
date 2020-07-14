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

 
   
def parse_morning():
    #_file='final/DX-Y.NYB.htm'
    outf='final/morning.html'
    objlist=['DX-Y.NYB','NFLX','FB','AMZN','GOOD','us10y-bond']
    df_out=pd.DataFrame()
    if os.path.exists(outf): 
        os.remove(outf)
    for obj in objlist:
        _file='final/{}.htm'.format(obj)
        if os.path.exists(_file): 
            print(lno(),_file)
            dfs = pd.read_html(_file,encoding = 'utf8')
            df=dfs[0].head(30)
            #print(lno(),df.iloc[0])
            #print(lno(),df.dtypes)
            cols=df.columns
            fix1=[]
            for i in cols:
                fix1.append('{}-{}'.format(obj,i))
            df.columns=fix1 
            if len(df_out)==0:
                df_out=df.copy()
            else:    
                df_out=pd.concat([df_out,df],axis=1)   
            #df.rename(columns={'日期':'{}日期'.format(obj)}, inplace=True)
    df_out=df_out.head(21)        
    html = df_out.to_html()
    #print(html)
    with open(outf, "a+", encoding="utf-8") as file:
        file.write(html)        

        
if __name__ == '__main__':
    #print (lno(),sys.path[0])
    #get_cur_twii_list(datetime.today())
    if len(sys.argv)==1:
        startdate=stock_comm.get_date()
        parse_morning()
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
        
        