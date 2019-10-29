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
import numpy as np
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, Float, Integer    
from sqlalchemy import Table, Column,  String, MetaData, ForeignKey    
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
                
def down_twse_3big(startdate,enddate):
    result=[]
    sr_list=[]
    dst_folder='csv/big3'
    check_dst_folder(dst_folder)
    nowdatetime = enddate
    day=0
    while   nowdatetime>=startdate :
        #print (lno(),nowdatetime)
        dstpath='%s/%d%02d%02d.csv'%(dst_folder,int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
        url='http://www.twse.com.tw/fund/BFI82U?response=csv&dayDate=%d%02d%02d&type=day'%(int(nowdatetime.year),int(nowdatetime.month),int(nowdatetime.day))
        download_file(url,dstpath)
        """
        if os.path.exists(dstpath):   
            df = pd.read_csv(dstpath,encoding = 'big5',skiprows=1)
            df.dropna(axis=1,how='all',inplace=True)
            df.dropna(inplace=True)
            print(lno(),len(df),df)
            #產生 日期,
        """    
            
        time.sleep(3)
        day=day+1
        nowdatetime = enddate - relativedelta(days=day)
def generate_twse_3big(startdate,enddate):
    out_file='csv/big3/big3_data.csv'
    dst_folder='csv/big3'
    check_dst_folder(dst_folder)
    nowdatetime = enddate
    day=0
    res=[]
    while   nowdatetime>=startdate :
        #print (lno(),nowdatetime)
        nowdate='%d%02d%02d'%(int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
        dstpath='%s/%s.csv'%(dst_folder,nowdate)

        if os.path.exists(dstpath):   
            df = pd.read_csv(dstpath,encoding = 'big5',skiprows=1)
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
            save_sql_day(nowdatetime)
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
                total=float(df.at[0,'外資total'].strip().replace(',', ''))
                #+float(df.at[0,'外資自營商total'].strip().replace(',', ''))
            except:
                print (lno(),df.at[0,'外資total'],df.at[0,'外資自營商total'])    
            total_int=int(total/100000000)
            print(lno(),date_str,df.at[0,'外資total'],df.at[0,'外資自營商total'],total)
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
                total1=float(df.at[0,'外資total'].strip().replace(',', ''))
                #+float(df.at[0,'外資自營商total'].strip().replace(',', ''))
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
def convert_currency_Billion(value):
    """
    转换字符串数字为float类型
     - 移除 ,
     - 转化为float类型
    """
    new_value = value.replace(',', '')
    return float(new_value)/100000000
    
def get_big3_df():
    out_file='csv/big3/big3_data.csv'
    #print(lno(),date)
    if os.path.exists(out_file): 
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        
        df_s['外資']=df_s['外資total'].apply(convert_currency_Billion)
        #+df_s['外資自營商total'].apply(convert_currency_Billion)
        df_s['投信']=df_s['投信total'].apply(convert_currency_Billion)
        df_s['自營商']=df_s['自營商total'].apply(convert_currency_Billion)+df_s['自營商避險total'].apply(convert_currency_Billion)
        df=df_s[['date','外資','投信','自營商']].copy()
        df.columns=['日期','外資','投信','自營商']
        #print(lno(), df_s[['date','外資','投信','自營商']])    
        #print(lno(),df_s[(df_s['date'] == date_str)].values.tolist())
        return df.round({'外資': 1, '投信': 1, '自營商': 1})

def big8_bill(value):
    """
    转换字符串数字为float类型
     - 移除 ,
     - 转化为float类型
    """
    #new_value = value.replace(',', '')
    return float(value)/100000
def download_big8():
    result=[]
    sr_list=[]
    dst_folder='csv/big3'
    check_dst_folder(dst_folder)
    #nowdatetime = enddate
    day=0

    url='http://chart.capital.com.tw/Chart/TWII/TAIEX11.aspx'
    filename='%s/test.csv'%(dst_folder)
    response = requests.get(url)
    # Check if the response is ok (200)
    if response.status_code == 200:
        # Open file and write the content
        print(len(response.content))
        if len(response.content)<10:
            return 0
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in response:
                file.write(chunk)
    f = open(filename, "r",encoding='UTF-8')
    webpage_text=f.read()
    #print (lno(),len(webpage_text))
    #if len(webpage_text)<400 and '查詢無資料' in webpage_text:
    #    print(lno(),'查詢無資料')
    #    return
    dfs = pd.read_html(StringIO(webpage_text))
    #for df in dfs:
    #    print(lno(),df)   
   
    """ 3.6.5 py
    df=dfs[0].copy()
    if df.iloc[1][0]=='日期':
        df.drop([0,1],inplace=True)
    """    
    if len(dfs)==3:
        df= dfs[1].copy()
        if df.iloc[0][0]=='日期':
            df.drop([0],inplace=True)
            df.dropna(axis=1,how='all',inplace=True)
            print(lno(),df.head())
            
            df1= dfs[2].copy()
            if df1.iloc[0][0]=='日期':
                df1.drop([0],inplace=True)
                df1.dropna(axis=1,how='all',inplace=True)
                df=pd.concat([df,df1],axis=0)
                print(lno(),df)
                #raise

    
        
        #
        #df=df.loc[df['日期']=='日期']
        #df=df[(df_s['date'] == date_str)]
        df.columns=['日期','八大行庫買賣超金額','台指期']
        df=df[(df['日期'] != '日期')] 
        #print(lno(),len(df),df['八大行庫買賣超金額'].values.tolist())

        dstpath='%s/big8_data.csv'%(dst_folder)
        
        if os.path.exists(dstpath):   
            df_s = pd.read_csv(dstpath,encoding = 'utf8',dtype={'日期': 'str','八大行庫買賣超金額': 'str','台指期': 'str'})
            df_s.dropna(axis=1,how='all',inplace=True)
            df_s.dropna(inplace=True)
            df_s.to_csv('csv/big3/ttt.csv',encoding='utf-8', index=False)
            print(lno(),len(df_s),df_s.tail())
            df_fin=pd.concat([df_s,df],axis=0)
            df_fin=df_fin.drop_duplicates(['日期'])
            print(lno(),len(df_fin),df_fin)
            df_fin['日期1'] =  pd.to_datetime(df_fin['日期'], format='%Y/%m/%d')
            df_fin=df_fin.sort_values(by=['日期1'], ascending=False)
            df_fin.drop('日期1',axis=1,inplace=True)
            df_fin.to_csv(dstpath,encoding='utf-8', index=False)
        #產生 日期,
    else:
        print(lno(),'8大資料 有問題')
        input('press key to cont:')  
            

def get_big8_df():
    out_file='csv/big3/big8_data.csv'
    #print(lno(),date)
    if os.path.exists(out_file): 
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        """
        df_s['外資']=df_s['外資total'].apply(convert_currency_Billion)+df_s['外資自營商total'].apply(convert_currency_Billion)
        df_s['投信']=df_s['投信total'].apply(convert_currency_Billion)
        df_s['自營商']=df_s['自營商total'].apply(convert_currency_Billion)+df_s['自營商避險total'].apply(convert_currency_Billion)
        df=df_s[['date','外資','投信','自營商']].copy()
        df.columns=['日期','外資','投信','自營商']
        """
        df_s['八大行庫買賣超金額']=df_s['八大行庫買賣超金額'].apply(big8_bill)
        df_s.columns=['日期','八大行庫','台指期']
        print(lno(), df_s)    
        #print(lno(),df_s[(df_s['date'] == date_str)].values.tolist())
        return df_s.round({'八大行庫': 1})
        
class twse_big3:
    def __init__(self):
        self.engine = create_engine('sqlite:///sql/twse_big3.db', echo=False)
        self.con = self.engine.connect()
        self.datafolder='csv/big3'
        
    def csv2sql_day(self,date):
        #print(lno(),date)
        filen='%s/%d%02d%02d.csv'%(self.datafolder,date.year,date.month,date.day)
        print(lno(),filen)
        if not os.path.exists(filen): 
            return
        df_fini = pd.DataFrame(columns=['買進金額', '賣出金額', '買賣差額'], dtype=np.int64) # 外資數據
        df_fund = pd.DataFrame(columns=['買進金額', '賣出金額', '買賣差額'], dtype=np.int64) # 投信數據
        df_prop_hedge = pd.DataFrame(columns=['買進金額', '賣出金額', '買賣差額'], dtype=np.int64) # 自營商(避險)數據
        df_prop_self = pd.DataFrame(columns=['買進金額', '賣出金額', '買賣差額'], dtype=np.int64) # 自營商(自行買賣)數據
        df_fini_prop = pd.DataFrame(columns=['買進金額', '賣出金額', '買賣差額'], dtype=np.int64) # 外資自營商數據
        df_sum = pd.DataFrame(columns=['買進金額', '賣出金額', '買賣差額'], dtype=np.int64) # 合計數據    
        if date >= datetime(2017, 12, 18):  # 證交所將「外資自營商」從「外資及陸資」當中分離出來
            df = pd.read_csv(filen,encoding = 'big5', header=1, index_col=0, usecols=[0,1,2,3], dtype={0:str, 1:np.int64, 2:np.int64, 3:np.int64}, skiprows=range(8, 50), thousands=',') # 依格式讀取資料
            df_fini.loc[date] = df.loc['外資及陸資(不含外資自營商)'] + df.loc['外資自營商']
            df_fini_prop.loc[date] = df.loc['外資自營商']
            df_prop_hedge.loc[date] = df.loc['自營商(避險)']
            df_prop_self.loc[date] = df.loc['自營商(自行買賣)']
            df_sum.loc[date] = df.loc['合計']
        elif date > datetime(2014, 11, 28): 
            df = pd.read_csv(filen,encoding = 'big5', header=1, index_col=0, usecols=[0, 1, 2, 3], dtype={0:str, 1:np.int64, 2:np.int64, 3:np.int64}, skiprows=range(7, 50), thousands=',') # 依格式讀取資料
            df_fini.loc[date] = df.loc['外資及陸資']
            #df_fini_prop.loc[date] = df.loc['外資自營商']
            df_prop_hedge.loc[date] = df.loc['自營商(避險)']
            df_prop_self.loc[date] = df.loc['自營商(自行買賣)']
            df_sum.loc[date] = df.loc['合計']
        elif date >= datetime(2013, 10, 28): 
            df = pd.read_csv(filen,encoding = 'big5', header=1, index_col=0, usecols=[0, 1, 2, 3], dtype={0:str, 1:np.int64, 2:np.int64, 3:np.int64}, skiprows=range(6, 50), thousands=',') # 依格式讀取資料
            df_fini.loc[date] = df.loc['外資及陸資']
            df_prop_self.loc[date] = df.loc['自營商']
            df_sum.loc[date] = df.loc['合計']    
        elif date > datetime(2009, 4, 30): 
            df = pd.read_csv(filen,encoding = 'big5',header=1, index_col=0, usecols=[0, 1, 2, 3], dtype={0:str, 1:np.int64, 2:np.int64, 3:np.int64}, skiprows=range(6, 50), thousands=',') # 依格式讀取資料
            df_fini.loc[date] = df.loc['外資及陸資']
            df_prop_self.loc[date]=df.loc['自營商']
            df_sum.loc[date] = df.loc['合計']
        else:
            df = pd.read_csv(filen,encoding = 'big5',header=1, index_col=0, usecols=[0, 1, 2, 3], dtype={0:str, 1:np.int64, 2:np.int64, 3:np.int64}, skiprows=range(6, 50), thousands=',') # 依格式讀取資料
            df_fini.loc[date] = df.loc['外資']
            df_prop_self.loc[date]=df.loc['自營商']
            df_sum.loc[date] = df.loc['合計']    
        df_fund.loc[date] = df.loc['投信']
        df_tt = pd.read_sql('sum', self.engine, index_col='table_index', parse_dates=['table_index'])   
        if date in df_tt.index:
            print(lno(),"repeat",date)
            return
        print(lno(),df_fini)    
        df_fini.to_sql('fini', self.engine, if_exists='append', index_label='table_index',chunksize=10)
        df_fund.to_sql('fund', self.engine, if_exists='append', index_label='table_index',chunksize=10)
        df_prop_hedge.to_sql('prop_hedge', self.engine, if_exists='append', index_label='table_index',chunksize=10)
        df_prop_self.to_sql('prop_self', self.engine, if_exists='append', index_label='table_index',chunksize=10)
        df_fini_prop.to_sql('fini_prop', self.engine, if_exists='append', index_label='table_index',chunksize=10)
        df_sum.to_sql('sum', self.engine, if_exists='append', index_label='table_index',chunksize=10)        
        pass
    def csv2sql_all(self):
        df_fini = pd.DataFrame(columns=['買進金額', '賣出金額', '買賣差額'], dtype=np.int64) # 外資數據
        df_fund = pd.DataFrame(columns=['買進金額', '賣出金額', '買賣差額'], dtype=np.int64) # 投信數據
        df_prop_hedge = pd.DataFrame(columns=['買進金額', '賣出金額', '買賣差額'], dtype=np.int64) # 自營商(避險)數據
        df_prop_self = pd.DataFrame(columns=['買進金額', '賣出金額', '買賣差額'], dtype=np.int64) # 自營商(自行買賣)數據
        df_fini_prop = pd.DataFrame(columns=['買進金額', '賣出金額', '買賣差額'], dtype=np.int64) # 外資自營商數據
        df_sum = pd.DataFrame(columns=['買進金額', '賣出金額', '買賣差額'], dtype=np.int64) # 合計數據
        file_names = os.listdir(self.datafolder)
        for file_name in file_names:
            if not file_name.endswith('.csv'):
                continue
            if (len(file_name)!=12):
                continue
            #print(lno(),file_name,len(file_name))
            date_str='%s%s%s'%(file_name[0:4],file_name[4:6],file_name[6:8])
            date=datetime.strptime(date_str,'%Y%m%d')
            #print('{}/{}'.format(self.datafolder, file_name))
            if date >= datetime(2017, 12, 18):  # 證交所將「外資自營商」從「外資及陸資」當中分離出來
                df = pd.read_csv('{}/{}'.format(self.datafolder, file_name),encoding = 'big5', header=1, index_col=0, usecols=[0,1,2,3], dtype={0:str, 1:np.int64, 2:np.int64, 3:np.int64}, skiprows=range(8, 50), thousands=',') # 依格式讀取資料
                df_fini.loc[date] = df.loc['外資及陸資(不含外資自營商)'] + df.loc['外資自營商']
                df_fini_prop.loc[date] = df.loc['外資自營商']
                df_prop_hedge.loc[date] = df.loc['自營商(避險)']
                df_prop_self.loc[date] = df.loc['自營商(自行買賣)']
                df_sum.loc[date] = df.loc['合計']
            elif date > datetime(2014, 11, 28): 
                df = pd.read_csv('{}/{}'.format(self.datafolder, file_name),encoding = 'big5', header=1, index_col=0, usecols=[0, 1, 2, 3], dtype={0:str, 1:np.int64, 2:np.int64, 3:np.int64}, skiprows=range(7, 50), thousands=',') # 依格式讀取資料
                df_fini.loc[date] = df.loc['外資及陸資']
                #df_fini_prop.loc[date] = df.loc['外資自營商']
                df_prop_hedge.loc[date] = df.loc['自營商(避險)']
                df_prop_self.loc[date] = df.loc['自營商(自行買賣)']
                df_sum.loc[date] = df.loc['合計']
            elif date >= datetime(2013, 10, 28): 
                df = pd.read_csv('{}/{}'.format(self.datafolder, file_name),encoding = 'big5', header=1, index_col=0, usecols=[0, 1, 2, 3], dtype={0:str, 1:np.int64, 2:np.int64, 3:np.int64}, skiprows=range(6, 50), thousands=',') # 依格式讀取資料
                df_fini.loc[date] = df.loc['外資及陸資']
                df_prop_self.loc[date] = df.loc['自營商']
                df_sum.loc[date] = df.loc['合計']    
            elif date > datetime(2009, 4, 30): 
                df = pd.read_csv('{}/{}'.format(self.datafolder, file_name),encoding = 'big5',header=1, index_col=0, usecols=[0, 1, 2, 3], dtype={0:str, 1:np.int64, 2:np.int64, 3:np.int64}, skiprows=range(6, 50), thousands=',') # 依格式讀取資料
                df_fini.loc[date] = df.loc['外資及陸資']
                df_prop_self.loc[date]=df.loc['自營商']
                df_sum.loc[date] = df.loc['合計']
            else:
                df = pd.read_csv('{}/{}'.format(self.datafolder, file_name),encoding = 'big5',header=1, index_col=0, usecols=[0, 1, 2, 3], dtype={0:str, 1:np.int64, 2:np.int64, 3:np.int64}, skiprows=range(6, 50), thousands=',') # 依格式讀取資料
                df_fini.loc[date] = df.loc['外資']
                df_prop_self.loc[date]=df.loc['自營商']
                df_sum.loc[date] = df.loc['合計']    
            df_fund.loc[date] = df.loc['投信']    
        df_fini=df_fini.sort_index(ascending=True)
        df_fund=df_fund.sort_index(ascending=True)
        df_prop_hedge=df_prop_hedge.sort_index(ascending=True)
        df_prop_self=df_prop_self.sort_index(ascending=True)
        df_fini_prop=df_fini_prop.sort_index(ascending=True)
        df_sum=df_sum.sort_index(ascending=True)

        df_fini.to_sql('fini', self.engine, if_exists='replace', index_label='table_index',chunksize=10)
        df_fund.to_sql('fund', self.engine, if_exists='replace', index_label='table_index',chunksize=10)
        df_prop_hedge.to_sql('prop_hedge', self.engine, if_exists='replace', index_label='table_index',chunksize=10)
        df_prop_self.to_sql('prop_self', self.engine, if_exists='replace', index_label='table_index',chunksize=10)
        df_fini_prop.to_sql('fini_prop', self.engine, if_exists='replace', index_label='table_index',chunksize=10)
        df_sum.to_sql('sum', self.engine, if_exists='replace', index_label='table_index',chunksize=10)
        pass    
        

    def get_fini_df(self):
        df = pd.read_sql('fini', self.engine, index_col='table_index', parse_dates=['table_index'])   
        return df
    def get_fund_df(self):
        df = pd.read_sql('fund', self.engine, index_col='table_index', parse_dates=['table_index'])   
        return df
    def get_prop_self_df(self):
        df = pd.read_sql('prop_self', self.engine, index_col='table_index', parse_dates=['table_index'])   
        return df        
def test_sql():
    big3=twse_big3()
    #big3.csv2sql_all()
    big3.csv2sql_day(datetime(2019, 10, 22))
def save_sql_day(date):
    big3=twse_big3()
    big3.csv2sql_day(date)

if __name__ == '__main__':
    #print (lno(),sys.path[0])
    #get_cur_twii_list(datetime.today())
    if len(sys.argv)==1:
        startdate=stock_comm.get_date()
        enddate=stock_comm.get_date()
        down_twse_3big(startdate,enddate)
        generate_twse_3big(startdate,enddate)
        download_big8()
        #get_list_form_url_and_save(url,dstpath)
        #show_twii(nowdatetime)
    elif sys.argv[1]=='-d' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==4 :

            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            down_twse_3big(startdate,enddate)
            generate_twse_3big(startdate,enddate) 

        elif len(sys.argv)==3 :
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            down_twse_3big(startdate,startdate)
            generate_twse_3big(startdate,startdate)
            download_big8()
        else:    
            print(lno(),'fun -d startdate enddate')
    elif sys.argv[1]=='-p' :
        print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            #參數2:開始日期  參數3:結束日期
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            generate_twse_3big(startdate,enddate)  
            

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
    elif sys.argv[1]=='big8' :        
        df=download_big8()
        #df=get_big8_df()
        print(lno(),df)
    elif sys.argv[1]=='sql' :        
        test_sql()
        
    else:   
        objdatetime=datetime.strptime(sys.argv[1],'%Y%m%d')
        df=get_big8_df()
        print(lno(),df)
       
        