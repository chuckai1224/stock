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

                
    
TSE_KLINE_PATH='csv/tse_kline'                
def download_file(url, filename):
    ''' Downloads file from the url and save it as filename '''
    # check if file already exists
    print('Downloading File')
    response = requests.get(url)
    # Check if the response is ok (200)
    if response.status_code == 200:
        # Open file and write the content
        #print(len(response.content))
        if len(response.content)<10:
            return 0
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in response:
                file.write(chunk)
        return 1
    return 0    
                
def down_otc_3big(startdate,enddate):
    result=[]
    sr_list=[]
    dst_folder='csv/big3'
    check_dst_folder(dst_folder)
    nowdatetime = enddate
    day=0
    while   nowdatetime>=startdate :
        #print (lno(),nowdatetime)
        dstpath='%s/%d%02d%02d_otc.csv'%(dst_folder,int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
        url='https://www.tpex.org.tw/web/stock/3insti/3insti_summary/3itrdsum_result.php?l=zh-tw&t=D&p=1&d=%d/%02d/%02d&o=htm'%(int(nowdatetime.year)-1911,int(nowdatetime.month),int(nowdatetime.day))
        tb = pd.read_html(url)
        #print(lno(),type(tb))
        if  isinstance(tb[0], pd.DataFrame):
            if  not tb[0].empty :
                print(lno(),len(tb),tb)
                tb[0].to_csv(dstpath, mode='w', encoding='utf_8', header=1, index=0)
        #download_file(url,dstpath)
        """
        if os.path.exists(dstpath):   
            df = pd.read_csv(dstpath,encoding = 'big5',skiprows=1)
            df.dropna(axis=1,how='all',inplace=True)
            df.dropna(inplace=True)
            print(lno(),len(df),df)
            #產生 日期,
        """    
            
        time.sleep(2)
        day=day+1
        nowdatetime = enddate - relativedelta(days=day)
def generate_otc_3big(startdate,enddate):
    out_file='csv/big3/big3_data_otc.csv'
    dst_folder='csv/big3'
    check_dst_folder(dst_folder)
    nowdatetime = enddate
    day=0
    res=[]
    while   nowdatetime>=startdate :
        #print (lno(),nowdatetime)
        nowdate='%d%02d%02d'%(int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
        dstpath='%s/%s_otc.csv'%(dst_folder,nowdate)

        if os.path.exists(dstpath):   
            df = pd.read_csv(dstpath,encoding = 'utf8',skiprows=1)
            print(lno(),df)
            df.dropna(axis=1,how='all',inplace=True)
            df.dropna(inplace=True)
            if len(df)==6:
                tmp=[]
                date_str='%d-%02d-%02d'%(int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
                tmp.append(date_str)
                for i in range(0, len(df)):
                    tmp.append(df.at[i,'買進金額'])
                    tmp.append(df.at[i,'賣出金額'])
                    tmp.append(df.at[i,'買賣差額'])
                    
                #print(lno(),tmp)
                res.append(tmp)
            elif len(df)==4 :
               
                tmp=[]
                date_str='%d-%02d-%02d'%(int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
                tmp.append(date_str)
                tmp.append(df.at[0,'買進金額'])
                tmp.append(df.at[0,'賣出金額'])
                tmp.append(df.at[0,'買賣差額'])
                tmp.append(0)
                tmp.append(0)
                tmp.append(0)
                tmp.append(df.at[1,'買進金額'])
                tmp.append(df.at[1,'賣出金額'])
                tmp.append(df.at[1,'買賣差額'])
                tmp.append(df.at[2,'買進金額'])
                tmp.append(df.at[2,'賣出金額'])
                tmp.append(df.at[2,'買賣差額'])
                tmp.append(0)
                tmp.append(0)
                tmp.append(0)
                tmp.append(df.at[3,'買進金額'])
                tmp.append(df.at[3,'賣出金額'])
                tmp.append(df.at[3,'買賣差額'])
                
                    
                #print(lno(),tmp)
                res.append(tmp)
            else :
                print(lno(),len(df),nowdatetime)
                tmp=[]
                date_str='%d-%02d-%02d'%(int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
                tmp.append(date_str)
                tmp.append(df.at[0,'買進金額'])
                tmp.append(df.at[0,'賣出金額'])
                tmp.append(df.at[0,'買賣差額'])
                tmp.append(df.at[1,'買進金額'])
                tmp.append(df.at[1,'賣出金額'])
                tmp.append(df.at[1,'買賣差額'])
                tmp.append(df.at[2,'買進金額'])
                tmp.append(df.at[2,'賣出金額'])
                tmp.append(df.at[2,'買賣差額'])
                
                tmp.append(df.at[3,'買進金額'])
                tmp.append(df.at[3,'賣出金額'])
                tmp.append(df.at[3,'買賣差額']) 
                tmp.append(0)
                tmp.append(0)
                tmp.append(0)
                tmp.append(df.at[4,'買進金額'])
                tmp.append(df.at[4,'賣出金額'])
                tmp.append(df.at[4,'買賣差額'])
                res.append(tmp)    
                #return []
            #產生 日期,
        day=day+1
        nowdatetime = enddate - relativedelta(days=day)        
    labels = ['date','自營商buy', '自營商sell', '自營商total', '自營商避險buy', '自營商避險sell', '自營商避險total','投信buy', '投信sell', '投信total', \
            '外資buy', '外資sell', '外資total','外資自營商buy', '外資自營商sell', '外資自營商total','總buy', '總sell', '總total',]

    res_df = pd.DataFrame.from_records(res, columns=labels)   
    #print (lno(),res_df)    
    if os.path.exists(out_file): 
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print(lno(),df_s['date'].dtype)
        #print(lno(),res_df['date'].dtype)
        df_s=df_s.append(res_df,ignore_index=True)
        #print(lno(),df_s[['date','自營商total']])
        df_s.drop_duplicates(subset=['date'],keep='first',inplace=True)
        #print(lno(),df_s[['date','外資total','投信total','自營商total']])
        df_s.to_csv(out_file,encoding='utf-8', index=False)
        
    else :
        res_df.to_csv(out_file,encoding='utf-8', index=False)
     
def get_twse_3big(date):
    out_file='csv/big3/big3_data.csv'
    #print(lno(),date)
    if os.path.exists(out_file): 
        
        date_str='%d-%02d-%02d'%(int(date.year), int(date.month),int(date.day))
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print(lno(),df_s[(df_s['date'] == date_str)].values.tolist())
        return df_s[(df_s['date'] == date_str)]

        
    else :
        return []  
def get_foreign_investment(date):
    out_file='csv/big3/big3_data.csv'
    #print(lno(),date)
    if os.path.exists(out_file): 
        
        date_str='%d-%02d-%02d'%(int(date.year), int(date.month),int(date.day))
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print(lno(),df_s[(df_s['date'] == date_str)].values.tolist())
        df=df_s[(df_s['date'] == date_str)]
        if len(df)==1:
            df.reset_index(inplace=True)
            #total=float(df.iat[0,'外資total'].strip().replace(',', ''))
            try:
                total=float(df.at[0,'外資total'].strip().replace(',', ''))+float(df.at[0,'外資自營商total'].strip().replace(',', ''))
            except:
                print (lno(),df.at[0,'外資total'],df.at[0,'外資自營商total'])    
            total_int=int(total/100000000)
            print(lno(),date_str,df.at[0,'外資total'],df.at[0,'外資自營商total'])
            return total_int
        return 0
        
    else :
        return 0   
def get_twse_big3_investmentent(date):
    out_file='csv/big3/big3_data.csv'
    #print(lno(),date)
    if os.path.exists(out_file): 
        
        date_str='%d-%02d-%02d'%(int(date.year), int(date.month),int(date.day))
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print(lno(),df_s[(df_s['date'] == date_str)].values.tolist())
        df=df_s[(df_s['date'] == date_str)]
        if len(df)==1:
            df.reset_index(inplace=True)
            #total=float(df.iat[0,'外資total'].strip().replace(',', ''))
            try:
                total1=float(df.at[0,'外資total'].strip().replace(',', ''))+float(df.at[0,'外資自營商total'].strip().replace(',', ''))
                total2=float(df.at[0,'自營商total'].strip().replace(',', ''))+float(df.at[0,'自營商避險total'].strip().replace(',', ''))
                total3=float(df.at[0,'投信total'].strip().replace(',', ''))
            except:
                print (lno(),df.at[0,'外資total'],df.at[0,'外資自營商total'])    
            total1_int=int(total1/100000000)
            total2_int=int(total2/100000000)
            total3_int=int(total3/100000000)
            #print(lno(),date_str,df.at[0,'外資total'],df.at[0,'外資自營商total'])
            return total1_int,total2_int,total3_int
        return 0
        
    else :
        return 0          
if __name__ == '__main__':
    #print (lno(),sys.path[0])
    #get_cur_twii_list(datetime.today())
    if len(sys.argv)==1:
        startdate=stock_comm.get_date()
        enddate=stock_comm.get_date()
        down_otc_3big(startdate,enddate)
        generate_otc_3big(startdate,enddate)
        #get_list_form_url_and_save(url,dstpath)
        #show_twii(nowdatetime)
    elif sys.argv[1]=='-d' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==4 :

            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            #down_otc_3big(startdate,enddate)
            generate_otc_3big(startdate,enddate) 

        else :
            
            print(lno(),'fun -d startdate enddate')
    elif sys.argv[1]=='-p' :
        print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            #參數2:開始日期  參數3:結束日期
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            generate_otc_3big(startdate,enddate)  
            

        else :
            print (lno(),'func -p startdata enddate')  
    elif sys.argv[1]=='-g' :
        print (lno(),len(sys.argv))
        if len(sys.argv)==3 :
            #參數2:開始日期 
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            foreign =get_foreign_investment(startdate)
            #df['外資buy']=df['外資buy'].astype('float64')            
            
            print(lno(),foreign)
            

        else :
            print (lno(),'func -g date')         
    else:   
        objdatetime=datetime.strptime(sys.argv[1],'%Y%m%d')
       
        