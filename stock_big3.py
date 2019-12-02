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
from io import StringIO
from matplotlib import pyplot as plt
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

                
    

def strip_dot(value):
    """
    转换字符串数字为int类型
     - 移除 ,
     - int
    """
    if type(value)==str:
        new_value = value.strip().replace(',', '')
        return int(new_value)
    else:
        return int(value)
def down_stock_3big(startdate,enddate,download=1):
    result=[]
    sr_list=[]
    dst_folder='csv/stock_big3'
    check_dst_folder(dst_folder)
    nowdatetime = enddate
    day=0
    while   nowdatetime>=startdate :
        print (lno(),nowdatetime)
        dstpath='%s/%d%02d%02d.csv'%(dst_folder,int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
        htmlfile='%s/html/%d%02d%02d.html'%(dst_folder,int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
        url='https://www.twse.com.tw/fund/T86?response=csv&date=%d%02d%02d&selectType=ALLBUT0999'%(int(nowdatetime.year),int(nowdatetime.month),int(nowdatetime.day))
        if download==1:
            response = requests.get(url)
            # Check if the response is ok (200)
            if response.status_code == 200:
                # Open file and write the content
                with open(htmlfile, 'wb') as file:
                    for chunk in response:
                        file.write(chunk)
        


        if os.path.exists(htmlfile):   
            #df = pd.read_csv(htmlfile,encoding = 'big5hkscs',skiprows=1)
            #print(lno(),df)
            try:
                df = pd.read_csv(htmlfile,encoding = 'big5hkscs',skiprows=1)
                df.dropna(axis=1,how='all',inplace=True)
                df.dropna(inplace=True)
                list=['證券代號','證券名稱','外陸資買進股數(不含外資自營商)','外陸資賣出股數(不含外資自營商)','外陸資買賣超股數(不含外資自營商)',
                '外資自營商買進股數','外資自營商賣出股數','外資自營商買賣超股數',
                '投信買進股數','投信賣出股數','投信買賣超股數',
                '自營商買進股數(自行買賣)','自營商賣出股數(自行買賣)','自營商買賣超股數(自行買賣)',
                '自營商買進股數(避險)','自營商賣出股數(避險)','自營商買賣超股數(避險)','自營商買賣超股數','三大法人買賣超股數']

                print(lno(),len(df.columns),df.columns)
                if len(df.columns)==16:
                    print(lno())
                    df.columns=['證券代號', '證券名稱', '外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)', '投信買進股數', '投信賣出股數','投信買賣超股數', '自營商買賣超股數', '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)','自營商買賣超股數(自行買賣)', '自營商買進股數(避險)', '自營商賣出股數(避險)', '自營商買賣超股數(避險)','三大法人買賣超股數']
                    print(lno())
                    df['外資自營商買進股數']=0
                    df['外資自營商賣出股數']=0
                    df['外資自營商買賣超股數']=0

                print(lno(),df.columns)
                
                df['證券代號']=[x.strip().replace('=', '').replace('\"','') for x in df['證券代號'] ]
                print(lno())
                df['證券名稱']=[x.strip() for x in df['證券名稱'] ]
                #print (lno(),df.dtypes)
                print(lno())
                df['外陸資買進股數(不含外資自營商)']=df['外陸資買進股數(不含外資自營商)'].apply(strip_dot)
                df['外陸資賣出股數(不含外資自營商)']=df['外陸資賣出股數(不含外資自營商)'].apply(strip_dot)
                df['外陸資買賣超股數(不含外資自營商)']=df['外陸資買賣超股數(不含外資自營商)'].apply(strip_dot)
                print (lno(),df['外資自營商買進股數'].dtype)
                
                df['外資自營商買進股數']= df['外資自營商買進股數'].apply(strip_dot)
                df['外資自營商賣出股數']=df['外資自營商賣出股數'].apply(strip_dot)
                df['外資自營商買賣超股數']=df['外資自營商買賣超股數'].apply(strip_dot)
                df['投信買進股數']=df['投信買進股數'].apply(strip_dot)
                df['投信賣出股數']=df['投信賣出股數'].apply(strip_dot)
                df['投信買賣超股數']=df['投信買賣超股數'].apply(strip_dot)
                df['自營商買賣超股數']=df['自營商買賣超股數'].apply(strip_dot)
                df['自營商買進股數(自行買賣)']=df['自營商買進股數(自行買賣)'].apply(strip_dot)
                df['自營商賣出股數(自行買賣)']=df['自營商賣出股數(自行買賣)'].apply(strip_dot)
                df['自營商買賣超股數(自行買賣)']=df['自營商買賣超股數(自行買賣)'].apply(strip_dot)
                print(lno())
                
                df['自營商買進股數(避險)']=df['自營商買進股數(避險)'].apply(strip_dot)
                df['自營商賣出股數(避險)']=df['自營商賣出股數(避險)'].apply(strip_dot)
                df['自營商買賣超股數(避險)']=df['自營商買賣超股數(避險)'].apply(strip_dot)
                print(lno())
                df['三大法人買賣超股數']=df['三大法人買賣超股數'].apply(strip_dot)
                print(lno())
                df1=df[list]
                print(lno())
                if len(df1)>0:
                    print(lno(),len(df1),df1)
                    df1.to_csv(dstpath,encoding='utf-8', index=False)
            except:
                print(lno(),"open file error",htmlfile)
            #產生 日期,

        if download ==1:    
            time.sleep(3)
        day=day+1
        nowdatetime = enddate - relativedelta(days=day)
def down_stock_3big_otc(startdate,enddate,download=1):
    result=[]
    sr_list=[]
    dst_folder='csv/stock_big3'
    check_dst_folder(dst_folder)
    nowdatetime = enddate
    day=0
    while   nowdatetime>=startdate :
        print (lno(),nowdatetime)
        dstpath='%s/%d%02d%02d_otc.csv'%(dst_folder,int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
        htmlfile='%s/html/%d%02d%02d_otc.html'%(dst_folder,int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
        changedate=datetime.strptime('20141201','%Y%m%d')
        if nowdatetime>=changedate:
            url='https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=csv&se=EW&t=D&d=%d/%02d/%02d&s=0,asc,0'%(int(nowdatetime.year)-1911,int(nowdatetime.month),int(nowdatetime.day))
        else :    
            url='https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_print.php?l=zh-tw&se=EW&t=D&d=%d/%02d/%02d&s=0,asc,0'%(int(nowdatetime.year)-1911,int(nowdatetime.month),int(nowdatetime.day))
        print(lno(),url)
        headers = {"User-Agent":"Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36"}
        if download==1:
            response = requests.get(url,headers=headers, stream=True)
            # Check if the response is ok (200)
            print(lno(),response.status_code)
            if response.status_code == 200:
                # Open file and write the content
                
                with open(htmlfile, 'wb') as file:
                    for chunk in response:
                        file.write(chunk)
        


        if os.path.exists(htmlfile):   
            print(lno(),"test")
           
            try:
                #df = pd.read_csv(htmlfile,encoding = 'big5hkscs',skiprows=1)
                if nowdatetime>=changedate:
                    df = pd.read_csv(htmlfile,encoding = 'big5hkscs',skiprows=1)
                else :
                    dfs = pd.read_html(htmlfile)
                    #print(lno(),len(dfs))
                    #print(lno(),dfs)
                    #"""
                    df=dfs[0]
                    df.columns=['證券代號', '證券名稱', 
                                '外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)',
                                 '投信買進股數', '投信賣出股數', '投信買賣超股數', 
                                 '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)', '自營商買賣超股數(自行買賣)',
                                  '三大法人買賣超股數']
                    #"""
                print(lno(),df.columns)
                list=['證券代號','證券名稱','外陸資買進股數(不含外資自營商)','外陸資賣出股數(不含外資自營商)','外陸資買賣超股數(不含外資自營商)',
                '外資自營商買進股數','外資自營商賣出股數','外資自營商買賣超股數',
                '投信買進股數','投信賣出股數','投信買賣超股數',
                '自營商買進股數(自行買賣)','自營商賣出股數(自行買賣)','自營商買賣超股數(自行買賣)',
                '自營商買進股數(避險)','自營商賣出股數(避險)','自營商買賣超股數(避險)','自營商買賣超股數','三大法人買賣超股數']
                print(lno(),len(df.columns),len(list))
                if len(df.columns)==12:
                    df['外資自營商買進股數']=0
                    df['外資自營商賣出股數']=0
                    df['外資自營商買賣超股數']=0
                    df['自營商買進股數(避險)']=0
                    df['自營商賣出股數(避險)']=0
                    df['自營商買賣超股數(避險)']=0
                    df['自營商買賣超股數']=df['自營商買賣超股數(自行買賣)']
                    df['證券代號']= df['證券代號'].astype('str')
                    df=df[list]
                elif len(df.columns)==16:
                    df.columns=['證券代號', '證券名稱', 
                                '外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)',
                                '投信買進股數', '投信賣出股數', '投信買賣超股數',
                                '自營商買賣超股數',
                                '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)', '自營商買賣超股數(自行買賣)',
                                '自營商買進股數(避險)','自營商賣出股數(避險)', '自營商買賣超股數(避險)', '三大法人買賣超股數']
                    df['外資自營商買進股數']=0
                    df['外資自營商賣出股數']=0
                    df['外資自營商買賣超股數']=0
                    df['證券代號']= df['證券代號'].astype('str')
                    df=df[list]
                else:
                    df.drop('外資及陸資-買進股數', axis=1, inplace = True)
                    df.drop('外資及陸資-賣出股數', axis=1, inplace = True)
                    df.drop('外資及陸資-買賣超股數', axis=1, inplace = True)
                    df.drop('自營商-買進股數', axis=1, inplace = True)
                    df.drop('自營商-賣出股數', axis=1, inplace = True)
                print(lno())
                df.dropna(axis=1,how='all',inplace=True)
                df.dropna(inplace=True)
                df.columns=list
                print(lno(),df['證券代號'])
                #"""
                df['證券代號']=[x.strip().replace('=', '').replace('\"','') for x in df['證券代號'] ]
                print(lno())
                df['證券名稱']=[x.strip() for x in df['證券名稱'] ]
                #print (lno(),df.dtypes)
                
                df['外陸資買進股數(不含外資自營商)']=df['外陸資買進股數(不含外資自營商)'].apply(strip_dot)
                df['外陸資賣出股數(不含外資自營商)']=df['外陸資賣出股數(不含外資自營商)'].apply(strip_dot)
                df['外陸資買賣超股數(不含外資自營商)']=df['外陸資買賣超股數(不含外資自營商)'].apply(strip_dot)
                print (lno(),df['外資自營商買進股數'].dtype)
                
                df['外資自營商買進股數']= df['外資自營商買進股數'].apply(strip_dot)
                df['外資自營商賣出股數']=df['外資自營商賣出股數'].apply(strip_dot)
                df['外資自營商買賣超股數']=df['外資自營商買賣超股數'].apply(strip_dot)
                df['投信買進股數']=df['投信買進股數'].apply(strip_dot)
                df['投信賣出股數']=df['投信賣出股數'].apply(strip_dot)
                df['投信買賣超股數']=df['投信買賣超股數'].apply(strip_dot)
                df['自營商買賣超股數']=df['自營商買賣超股數'].apply(strip_dot)
                df['自營商買進股數(自行買賣)']=df['自營商買進股數(自行買賣)'].apply(strip_dot)
                df['自營商賣出股數(自行買賣)']=df['自營商賣出股數(自行買賣)'].apply(strip_dot)
                df['自營商買賣超股數(自行買賣)']=df['自營商買賣超股數(自行買賣)'].apply(strip_dot)
                
                
                df['自營商買進股數(避險)']=df['自營商買進股數(避險)'].apply(strip_dot)
                df['自營商賣出股數(避險)']=df['自營商賣出股數(避險)'].apply(strip_dot)
                df['自營商買賣超股數(避險)']=df['自營商買賣超股數(避險)'].apply(strip_dot)
                
                df['三大法人買賣超股數']=df['三大法人買賣超股數'].apply(strip_dot)
                df1=df[list]
                if len(df1)>0:
                    print(lno(),len(df1),df1)
                    df1.to_csv(dstpath,encoding='utf-8', index=False)
                #"""    
            except:
                print(lno(),"open file error",htmlfile)
            #產生 日期,

            
        time.sleep(1)
        day=day+1
        nowdatetime = enddate - relativedelta(days=day)
        
def convert_K_stock(value):
    """
    轉換股數單位千股

    """
    return int(value/1000)
    
def get_stock_3big(stock_id,date,num,flag='tse',ver=1):
    if ver==1:
        sb3=stock_big3()
        return sb3.get_df_by_id_date_num(stock_id,date,num)
    dst_folder='csv/stock_big3'
    #print(lno(),date)
    if comm.stock_is_otc(stock_id,date)==1:
        flag='otc'
    nowdatetime = date
    day=0
    rec=0
    tmp_list=[]
    while   rec<num :
        nowdate = date - relativedelta(days=day)
        date_str='%d-%02d-%02d'%(int(nowdate.year), int(nowdate.month),int(nowdate.day))
        out_file='%s/%d%02d%02d.csv'%(dst_folder,int(nowdate.year), int(nowdate.month),int(nowdate.day))
        if flag=='otc':
            out_file='%s/%d%02d%02d_otc.csv'%(dst_folder,int(nowdate.year), int(nowdate.month),int(nowdate.day))
        day=day+1
        if os.path.exists(out_file): 
            
            df_s = pd.read_csv(out_file,encoding = 'utf-8')
            df_s.dropna(axis=1,how='all',inplace=True)
            df_s.dropna(inplace=True)
            #print(lno(),df_s[(df_s['date'] == date_str)].values.tolist())
            #print(lno(),df_s)
            df= df_s[(df_s['證券代號'] == stock_id)]

            df1=df[['外陸資買賣超股數(不含外資自營商)','投信買賣超股數','自營商買賣超股數','三大法人買賣超股數']]
            tt1=[]
            tt1.append(date_str)
            if len(df1)==1:
                tt1.extend(df1.iloc[0].values.tolist())
            else:
                tt1.extend([0,0,0,0])
            #df= df_s[(df_s['證券代號'] == '3481')]
            #print(lno(),tt1)
            tmp_list.append(tt1)
            rec=rec+1
        if day>num*2:
            break

    labels =['日期','外資','投信','自營商','三大法人買賣超']
    df_out = pd.DataFrame.from_records(tmp_list, columns=labels)
    
    #print(lno(),df_out)
    df_out['外資']=df_out['外資'].apply(convert_K_stock)
    df_out['投信']=df_out['投信'].apply(convert_K_stock)
    df_out['自營商']=df_out['自營商'].apply(convert_K_stock)
    #print(lno(),df_out)
    return df_out

def generate_stock_3big_pic(stock_no,enddate,outf,ver=1):
    df=get_stock_3big(stock_no,enddate,5)
    if ver==1:
        df=df.sort_values('日期', ascending=True).reset_index(drop=True)  
        df['日期']= [x.strftime('%Y%m%d') for x in df['日期'] ]  
    else:    
        df['date']=[comm.date_sub2time64(x) for x in df['日期'] ] 
        df=df.sort_values('date', ascending=True).reset_index(drop=True)    
    df_o=df[['日期','外資','投信','自營商']]
    index=df_o.index        
    bar_width = 0.3
    plt.figure(figsize=(10,8))
    A = plt.bar(index,df_o['外資'], bar_width, alpha=0.7,label="外資",color='red') 
    B = plt.bar(index+0.3,df_o['投信'],bar_width,alpha=0.7,label="投信",color='green') 
    C = plt.bar(index+0.6,df_o['自營商'],bar_width,alpha=0.7,label="自營商",color='blue') 
    def createLabels(data):
        for item in data:
            height = item.get_height()
            plt.text(
                item.get_x()+item.get_width()/2., 
                height*1.05, 
                '%.0f' % height,
                ha = "center",
                va = "bottom",
            )
    createLabels(A)
    createLabels(B)
    createLabels(C)
    plt.xticks(index+.3 / 2 ,df_o['日期'])
    plt.legend() 
    plt.grid(True)
    #plt.show()
    
    plt.savefig(outf)
    plt.clf()
    plt.close()  
 
def get_stock_3big_all(date,flag='tse',ver=1):
    if ver==1:
        sb3=stock_big3()
        return sb3.get_df_by_date(date)
    dst_folder='csv/stock_big3'
    #print(lno(),date)
    nowdate = date

    out_file='%s/%d%02d%02d.csv'%(dst_folder,int(nowdate.year), int(nowdate.month),int(nowdate.day))
    if flag=='otc':
        out_file='%s/%d%02d%02d_otc.csv'%(dst_folder,int(nowdate.year), int(nowdate.month),int(nowdate.day))
    if os.path.exists(out_file): 
        df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype= {'證券代號':str})
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #df1=df_s[['外陸資買賣超股數(不含外資自營商)','投信買賣超股數','自營商買賣超股數','三大法人買賣超股數']]
        return df_s
    return pd.DataFrame()
class stock_big3:
    def __init__(self):
        self.engine = create_engine('sqlite:///sql/stock_big3.db', echo=False)
        self.con = self.engine.connect()
        self.datafolder='csv/stock_big3'
        self.dtypes={0:str}
        self.columns=['證券代號','證券名稱', '外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)',
       '外資自營商買進股數', '外資自營商賣出股數', '外資自營商買賣超股數', '投信買進股數', '投信賣出股數', '投信買賣超股數',
       '自營商買賣超股數', '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)', '自營商買賣超股數(自行買賣)',
       '自營商買進股數(避險)', '自營商賣出股數(避險)', '自營商買賣超股數(避險)', '三大法人買賣超股數' ]
    def download_(self,date,market,download=1):
        if market=='tse':
            url='https://www.twse.com.tw/fund/T86?response=csv&date=%d%02d%02d&selectType=ALLBUT0999'%(int(date.year),int(date.month),int(date.day))
            htmlfile='%s/html/%s.html'%(self.datafolder,date.strftime('%Y%m%d'))
        else:
            changedate=datetime.strptime('20141201','%Y%m%d')
            htmlfile='%s/html/%s_otc.html'%(self.datafolder,date.strftime('%Y%m%d'))
            if date>=changedate:
                url='https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=csv&se=EW&t=D&d=%d/%02d/%02d&s=0,asc,0'%(int(date.year)-1911,int(date.month),int(date.day))
            else :    
                url='https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_print.php?l=zh-tw&se=EW&t=D&d=%d/%02d/%02d&s=0,asc,0'%(int(date.year)-1911,int(date.month),int(date.day))                
        if download==1:
            response = requests.get(url)
            if response.status_code == 200:
                with open(htmlfile, 'wb') as file:
                    for chunk in response:
                        file.write(chunk)
        else:
            csv=htmlfile
            
        if not os.path.exists(htmlfile):
            return
        print(lno(),date)
        try:
            df = pd.read_csv(htmlfile,encoding = 'big5hkscs',header=1,dtype=self.dtypes, thousands=',' )
            #print(lno(),len(df.columns))    
            #df = pd.read_csv(htmlfile,encoding = 'big5hkscs',header=1,usecols=columns,dtype=self.dtypes, thousands=',' )
        except:
            filesize=os.path.getsize(htmlfile)
            if  filesize<1024:
                os.remove(htmlfile)
                #print(lno(),"wrong file size", htmlfile)
                return
            else:
                if market == 'otc' and date <= datetime(2014, 12, 1):
                    dfs = pd.read_html(htmlfile)
                    df=dfs[0].copy()
                    #df.columns=['證券代號', '證券名稱', '外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)', '投信買進股數', '投信賣出股數', '投信買賣超股數', '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)', '自營商買賣超股數(自行買賣)', '三大法人買賣超股數']
                else :   
                    print(lno(),"wrong xxx", market,htmlfile,filesize)
                    df = pd.read_csv(htmlfile,encoding = 'big5hkscs',header=1,dtype=self.dtypes, thousands=',' )
                    print(lno(),df.columns)
                    print(lno(),len(df.columns))
                    raise    

        df=df.dropna(thresh=3)
        df.dropna(axis=1,how='all',inplace=True)
       
        if market=='otc':
            print(lno(),len(df.columns)) 
            print(lno(),df.columns) 
            if date < datetime(2014, 12, 1):
                columns=['證券代號', '證券名稱', 
                         '外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)', 
                         '投信買進股數', '投信賣出股數', '投信買賣超股數',
                         '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)', '自營商買賣超股數', '三大法人買賣超股數']
            elif date < datetime(2018, 1, 15):
                columns=['證券代號','證券名稱', 
                            '外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)', 
                            '投信買進股數', '投信賣出股數', '投信買賣超股數', 
                            '自營商買賣超股數',
                            '自營商買進股數(自行買賣)','自營商賣出股數(自行買賣)', '自營商買賣超股數(自行買賣)',
                            '自營商買進股數(避險)','自營商賣出股數(避險)','自營商買賣超股數(避險)', '三大法人買賣超股數']
            else:
                df.columns=['證券代號','證券名稱', 
                        '外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)',
                        '外資自營商買進股數', '外資自營商賣出股數', '外資自營商買賣超股數',
                        '外資及陸資-買進股數', '外資及陸資-賣出股數', '外資及陸資-買賣超股數', 
                        '投信買進股數', '投信賣出股數', '投信買賣超股數',
                        '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)', '自營商買賣超股數(自行買賣)',
                        '自營商買進股數(避險)', '自營商賣出股數(避險)', '自營商買賣超股數(避險)',
                        '自營商-買進股數', '自營商-賣出股數','自營商買賣超股數',
                         '三大法人買賣超股數' ]  
                df=df[self.columns]  
                columns=self.columns 
                
        else:  
            print(lno(),len(df.columns)) 
            print(lno(),df.columns)       
            if date < datetime(2014, 12, 1):
                
                columns=['證券代號','證券名稱', 
                            '外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)', 
                            '投信買進股數', '投信賣出股數', '投信買賣超股數', 
                            '自營商買進股數(自行買賣)','自營商賣出股數(自行買賣)','自營商買賣超股數',
                            '三大法人買賣超股數']
            elif date < datetime(2017, 12, 18):
                columns=['證券代號','證券名稱', 
                            '外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)', 
                            '投信買進股數', '投信賣出股數', '投信買賣超股數',
                            '自營商買賣超股數', 
                            '自營商買進股數(自行買賣)','自營商賣出股數(自行買賣)','自營商買賣超股數(自行買賣)',
                            '自營商買進股數(避險)', '自營商賣出股數(避險)', '自營商買賣超股數(避險)',
                            '三大法人買賣超股數']
            else:
                columns=self.columns    
                    
        df.columns=columns        
        df['date']=date
        df['market']=market
        df['證券代號']=[x.strip().replace('=', '').replace('\"','') for x in df['證券代號'] ]
        enddate=date+relativedelta(days=1)
        table_name=date.strftime('%Y%m%d')
        if table_name in self.engine.table_names():
            cmd='SELECT * FROM "{}" WHERE date >= "{}" and date < "{}" and market=="{}"'.format(table_name,date,enddate,market)
            df_query= pd.read_sql(cmd, con=self.con, parse_dates=['date'])
            if len(df_query):
                print(lno(),"repeat",date)
                return
            else:
                df.to_sql(name=table_name,  con=self.con, if_exists='append',  index= False,chunksize=10)
            
        else:    
            df.to_sql(name=table_name, con=self.con, if_exists='append',  index= False,chunksize=10)
           
        
    def download(self,date,download=1):
        self.download_(date,'tse',download)
        self.download_(date,'otc',download)
        if download==1:
            time.sleep(3)
    def download_by_dates(self,startdate,enddate,download=1):
        nowdate=startdate
        day=0
        while nowdate<=enddate:
            nowdate=startdate+relativedelta(days=day)
            self.download(nowdate,download)
            day=day+1             
    def get_df_by_date(self,date):
        table_name=date.strftime('%Y%m%d')
        cmd='SELECT * FROM "{}" '.format(table_name)
        try:
            df= pd.read_sql(cmd, con=self.con, parse_dates=['date'])
            return df
        except:
            return pd.DataFrame()
                
        
    def get_df_by_id_date(self,stock_id,date):
        table_name=date.strftime('%Y%m%d')
        cmd='SELECT * FROM "{}" WHERE "證券代號"=="{}" '.format(table_name,stock_id)
        df= pd.read_sql(cmd, con=self.con, parse_dates=['date'])
        #print(lno(),df)
        return df
    def get_df_by_id_date_num(self,stock_id,date,num):
        day=0
        rec=0
        table_names = self.engine.table_names()
        df_fin=pd.DataFrame()
        while   rec<num :
            nowdate = date - relativedelta(days=day)
            str_date=nowdate.strftime('%Y%m%d')
            if str_date in table_names:
                rec=rec+1
                df=self.get_df_by_id_date(stock_id,nowdate)
                if len(df.index):
                    df_fin=pd.concat([df_fin,df[['date','外陸資買賣超股數(不含外資自營商)','投信買賣超股數','自營商買賣超股數','三大法人買賣超股數']]])
            day=day+1
            if day>num*100:
                break
        if len(df_fin.index ):
            df_fin.columns=['日期','外資','投信','自營商','三大法人買賣超']
            df_fin['外資']=df_fin['外資']/1000
            df_fin['投信']=df_fin['投信']/1000
            df_fin['自營商']=df_fin['自營商']/1000
            df_fin['三大法人買賣超']=df_fin['三大法人買賣超']/1000
        print(lno(),df_fin)
        return df_fin

              
   
     
def test_sql(date,download):
    ss=stock_big3()
    #print(lno(),date)
    #ss.download(date,download)
    #"""
    #df=get_stock_3big_all(date,'tse')
    #print(lno(),df)
    def check_data(r):
        debug=0
        print(lno(),date,r['證券代號'])
        d1=ss.get_by_stock_id(r['證券代號'],date) 
        if len(d1.index)==1:
            #if float(r['外陸資買賣超股數(不含外資自營商)'])!=d1.iloc[0]['外陸資買賣超股數(不含外資自營商)']:
            #    debug=1
            if float(r['投信買賣超股數'])!=d1.iloc[0]['投信買賣超股數']:
                debug=1
        if debug==1:        
            print(lno(),date)
            #print(lno(),float(r['外陸資買賣超股數(不含外資自營商)']),d1.iloc[0]['外陸資買賣超股數(不含外資自營商)'])
            print(lno(),float(r['投信買賣超股數']),d1.iloc[0]['投信買賣超股數'])
            print(lno(),d1[['外陸資買賣超股數(不含外資自營商)','投信買賣超股數']])
            raise
    #df.apply(check_data,axis=1)
    #"""
    df=get_stock_3big_all(date,'otc')
    if len(df):
        print(lno(),df.columns)
        print(lno(),df[['證券代號','投信買賣超股數']])
    #raise
    df.apply(check_data,axis=1)
    
   
if __name__ == '__main__':
    #print (lno(),sys.path[0])
    #get_cur_twii_list(datetime.today())
    sb3=stock_big3()
    if len(sys.argv)==1:
        startdate=stock_comm.get_date()
        enddate=stock_comm.get_date()
        sb3.download_by_dates(startdate,enddate)
        #down_stock_3big(startdate,enddate)
        #down_stock_3big_otc(startdate,enddate)

    elif sys.argv[1]=='-d' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            sb3.download_by_dates(startdate,enddate)
            #down_stock_3big(startdate,enddate)
            #down_stock_3big_otc(startdate,enddate,download=0)
        else :
            print(lno(),'fun -d startdate enddate')
    elif sys.argv[1]=='-g' :
        print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            #參數2:開始日期 
            stock_id=sys.argv[2]
            startdate=datetime.strptime(sys.argv[3],'%Y%m%d')
            df =get_stock_3big(stock_id,startdate,5)
            
            #df['外資buy']=df['外資buy'].astype('float64')            
            if len(df):
                print(lno(),df[['外資','投信','自營商']])
                print(lno(),df.loc[:,'外資'].values.tolist())
            
            

        else :
            print (lno(),'func -g date')         
    elif sys.argv[1]=='pic' :        
        stock_no=sys.argv[2]
        enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        generate_stock_3big_pic(stock_no,enddate,'tes1.png')
    elif sys.argv[1]=='tt' :  
        try:
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')      
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        except:
            pass  
        stock_id='2308'
        date=startdate
        num=5
        df=sb3.get_df_by_id_date_num(stock_id,date,num)  
        print(lno(),df)
        df=sb3.get_df_by_date(date)  
        print(lno(),df)
       
        
        #print(lno(),df)
    elif sys.argv[1]=='-t' : 
        ##TODO: need to regen again for wrong data 自營商       
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        try:
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        except:    
            enddate=startdate
        nowdate=startdate
        day=0
        while nowdate<=enddate:
            nowdate=startdate+relativedelta(days=day)
            test_sql(nowdate,download=0)
            #time.sleep(3)
            day=day+1     
        
    else:   
        objdatetime=datetime.strptime(sys.argv[1],'%Y%m%d')

        #print(lno(),df)
       
        