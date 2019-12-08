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
import inspect
#import urllib2
#import lxml.html
#from bs4 import BeautifulSoup  
tdcc_file='../data/tdcc_date.csv'
from inspect import currentframe, getframeinfo
from datetime import datetime
from dateutil.relativedelta import relativedelta
import inspect
import traceback
DEBUG=1
LOG=1
import logging
import requests
import numpy as np
import pandas as pd
from math import ceil
import matplotlib
import fut
from matplotlib import pyplot as plt
import pdfkit as pdf
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
def tdcc_read_date(csv_path,objdate):
    with open(csv_path) as csv_file:
        csv_data = csv.reader(csv_file)
        result = []
        cnt=0
        for i in csv_data:
            if datetime.strptime(i[0],'%Y%m%d') <= datetime.strptime(objdate,'%Y%m%d'):
                #print i[0]
                result.append(datetime.strptime(i[0],'%Y%m%d').date())
                cnt+=1
                if(cnt>6):
                    break
        #print result   
        return result
def stock_dist_get(stock_no,objdate):
    dist = []
    try:
        with open(csv_path) as csv_file:
            csv_data = csv.reader(csv_file)
    except AssertionError:
            return dist
    return dist
"""    
def get_html(url):
    headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
    request = urllib2.Request(url,headers=headers)
    try:
        response = urllib2.urlopen(request)
    except urllib2.HTTPError  as e:
        print (lno(),e.code)
        print (lno(),e.read())
    else:
        html = response.read()
        return html

def get_post_html(url):
    headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
    request = urllib2.Request(url,headers=headers)
    try:
        response = urllib2.urlopen(request)
    except urllib2.HTTPError  as e:
        print e.code
        print e.read()
    else:
        html = response.read()
        return html
"""
def ReadCSVasList(csv_file):
    try:
        with open(csv_file) as csvfile:
            reader = csv.reader(csvfile, dialect='excel', quoting=csv.QUOTE_NONNUMERIC)
            datalist = []
            #print lno(),type(reader)
            tmp=[]
            for row in reader:
                tmp.append(row)
            datalist = list(tmp)
            #print datalist
            return datalist
    except IOError as errno :
            print("I/O ReadCSVasList error({0}): {1}".format(errno, strerror))  
            print (lno(),csv_file  )
    return
def WriteListToCSV(csv_file,csv_columns,data_list):
    try:
        with open(csv_file, 'w',newline='') as csvfile:
            writer = csv.writer(csvfile, dialect='excel', quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(csv_columns)
            for data in data_list:
                writer.writerow(data)
    except IOError as errno :
            print("I/O WriteListToCSV error({0}): {1}".format(errno, strerror))    
    return
def down_optData(startdate,enddate):
    dst_folder='csv/op'
    filename='csv/op/optmp.csv'
    
    check_dst_folder(dst_folder)
    startdate_str='%d/%02d/%02d'%(int(startdate.year), int(startdate.month),int(startdate.day))
    enddate_str='%d/%02d/%02d'%(int(enddate.year), int(enddate.month),int(enddate.day))
    #url = 'http://www.taifex.com.tw/cht/3/optDailyMarketReport'
    url = 'https://www.taifex.com.tw/cht/3/optDataDown '

    query_params = {
        'down_type': '1',
        'commodity_id':'TXO',
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
    
    try:
        df = pd.read_csv(filename,encoding = 'big5',skiprows=1,header=None)        
    except :
        print(lno(),startdate_str,enddate_str)
        return
    #df = pd.read_csv(filename,encoding = 'utf-8',skiprows=1,header=None)
    df.columns=['交易日期','契約','到期月份(週別)','履約價','買賣權','開盤價','最高價','最低價','收盤價','成交量','結算價','未沖銷契約數','最後最佳買價','最後最佳賣價','歷史最高價','歷史最低價','是否因訊息面暫停交易','交易時段','dummy']
    #print (lno(),df.index)

    df.dropna(axis=1,how='all',inplace=True)
    df.dropna(inplace=True)
    #df['交易日期']=[x.replace('  T', '').strip() for x in df['交易日期'] ]
    #df.reset_index(inplace=True)
    #df['日期'] =  pd.to_datetime(df['日期'], format='%Y/%m/%d')
    #print (lno(),df.iloc[0]['交易日期'],df['交易日期'].dtypes)
    nowdatetime = enddate
    day=0
    while   nowdatetime>=startdate :
        print (lno(),nowdatetime,startdate)
        index_date='%d/%02d/%02d'%(int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
        #print (lno(),index_date)
        out_file='csv/op/optData_%d%02d%02d.csv'%(int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
        df_now=df[(df['交易日期'] == index_date)]
        if len(df_now)>0:
            #print(lno(),len(df_now.columns))
            df_now.to_csv(out_file,encoding='utf-8', index=False)
        day=day+1 
        nowdatetime = enddate - relativedelta(days=day)
def down_op_pc(startdate,enddate):
    dst_folder='csv/op'
    filename='csv/op/tmp.csv'
    out_file='csv/op/op_data.csv'
    check_dst_folder(dst_folder)
    startdate_str='%d/%02d/%02d'%(int(startdate.year), int(startdate.month),int(startdate.day))
    enddate_str='%d/%02d/%02d'%(int(enddate.year), int(enddate.month),int(enddate.day))
    #url = 'http://www.taifex.com.tw/cht/3/optDailyMarketReport'
    url = 'http://www.taifex.com.tw/cht/3/pcRatioDown'
    query_params = {
        'down_type': '',
        'queryEndDate': enddate_str,
        'queryStartDate': startdate_str
    }

    page = requests.post(url, data=query_params)
    if not page.ok:
        print(lno(),"Can not get data at {}".format(url))
        return 
    with open(filename, 'wb') as file:
        # A chunk of 128 bytes
        for chunk in page:
            file.write(chunk)
    df = pd.read_csv(filename,encoding = 'utf-8',skiprows=1,header=None)
    df.columns=['日期','賣權成交量','買權成交量','買賣權成交量比率%','賣權未平倉量','買權未平倉量','買賣權未平倉量比率%','dummy']
    print (lno(),df)
    df.dropna(axis=1,how='all',inplace=True)
    df.dropna(inplace=True)
    #df['日期'] =  pd.to_datetime(df['日期'], format='%Y/%m/%d')
    print (lno(),df,df['日期'].dtypes)
    if os.path.exists(out_file): 
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        
        
        df_s=df_s.append(df,ignore_index=True)
        
        df_s.drop_duplicates(subset=['日期'],keep='first',inplace=True)
        df_s=df_s.sort_values(by=['日期'], ascending=False)
        
        df_s.to_csv(out_file,encoding='utf-8', index=False)
        
    else :
        df.to_csv(out_file,encoding='utf-8', index=False)

def down_opDelta(enddate):
    dst_folder='csv/op'
    filename='csv/op/op_delta_tmp.csv'
    out_file='csv/op/op_delta_%d%02d%02d.csv'%(int(enddate.year), int(enddate.month),int(enddate.day))
    check_dst_folder(dst_folder)
    enddate_str='%d/%02d/%02d'%(int(enddate.year), int(enddate.month),int(enddate.day))
    #url = 'http://www.taifex.com.tw/cht/3/optDailyMarketReport'
    url = 'https://www.taifex.com.tw/file/taifex/Dailydownload/delta/chinese/Delta_%d%02d%02d.csv'%(int(enddate.year), int(enddate.month),int(enddate.day))
    

    page = requests.get(url )
    if not page.ok:
        print(lno(),"Can not get data at {}".format(url))
        return 
    with open(filename, 'wb') as file:
        # A chunk of 128 bytes
        for chunk in page:
            file.write(chunk)
    
    try:
        df = pd.read_csv(filename,encoding = 'big5',skiprows=1,header=None)        
    except :
        print(lno(),startdate_str,enddate_str)
        return
    #df = pd.read_csv(filename,encoding = 'utf-8',skiprows=1,header=None)
    #df.columns=['交易日期','契約','到期月份(週別)','履約價','買賣權','開盤價','最高價','最低價','收盤價','成交量','結算價','未沖銷契約數','最後最佳買價','最後最佳賣價','歷史最高價','歷史最低價','是否因訊息面暫停交易','交易時段','dummy']
    print (lno(),df)
    if '商品' in df.iloc[0].values.tolist() :
        columns=df.iloc[0].values.tolist();
        df.columns=['商品','買賣權','到期月份(週別)','履約價格','Delta值']
        df=df.drop([0])
        df.dropna(axis=1,how='all',inplace=True)
        df.dropna(inplace=True)
        df['商品']=df['商品'].str.replace(" ","")
        df['買賣權']=df['買賣權'].str.replace(" ","")
        df['到期月份(週別)']=df['到期月份(週別)'].str.replace(" ","")
        df['履約價格']=df['履約價格'].str.replace(" ","")
        df['Delta值']=df['Delta值'].str.replace(" ","")
        #print (lno(),df)
        df.to_csv(out_file,encoding='utf-8', index=False)
   
def get_delta_ratio(date,BoS,MoW,price):
    date_str='%d%02d%02d'%(int(date.year), int(date.month),int(date.day))
    out_file='csv/op/op_delta_%d%02d%02d.csv'%(int(date.year), int(date.month),int(date.day))
    if os.path.exists(out_file): 
        date_str='%d/%02d/%02d'%(int(date.year), int(date.month),int(date.day))
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        df_s.columns=['商品', '買賣權', '到期月份(週別)', '履約價格', 'Delta值']
        df_s['履約價格']=df_s['履約價格'].astype('int64')
        df_s['到期月份(週別)']=df_s['到期月份(週別)'].astype('str')
        #print(lno(),type(price),df_s['履約價格'].dtype)
        #print(lno(),type(MoW),df_s['到期月份(週別)'].dtype)
        #print (lno(),df_s[(df_s['履約價格'] == price)])
        #print (lno(),df_s[(df_s['商品'] == 'TXO')])
        #print(lno(),date,BoS,MoW,price)
        df=df_s[ (df_s['履約價格'] == price)& (df_s['商品'] == 'TXO') & (df_s['到期月份(週別)'] == MoW)& (df_s['買賣權'] == BoS)]
        #print(lno(),df)
        if len(df)==1:
            df.reset_index(inplace=True)
            #print(lno(),df)
            return df.iloc[0]['Delta值']
    return 0        
def get_op_ratio(date):
    out_file='csv/op/op_data.csv'
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
                total=float(df.at[0,'買賣權未平倉量比率%'])
            except:
                print (lno(),df.at[0,'買賣權未平倉量比率%'])  
                total=0        
            
            #print(lno(),total_int)
            return total
        return 0
        
    else :
        return 0   
def get_op_daily(date):
    out_file='csv/op/op_data.csv'
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
            p_vol=0
            b_vol=0
            bp_vol_per=0.0
            p_oi=0
            b_oi=0
            bs_oi_per=0
            p_sub_b=0
            p_add_b=0
            try:
            #賣權成交量,買權成交量,買賣權成交量比率%,賣權未平倉量,買權未平倉量,買賣權未平倉量比率%
                p_vol=int(df.at[0,'賣權成交量'])
                b_vol=int(df.at[0,'買權成交量'])
                bp_vol_per=float(df.at[0,'買賣權成交量比率%'])
                p_oi=int(df.at[0,'賣權未平倉量'])
                b_oi=int(df.at[0,'買權未平倉量'])
                bs_oi_per=float(df.at[0,'買賣權成交量比率%'])
                p_sub_b=p_oi-b_oi
                p_add_b=p_oi+b_oi
                
            except:
                print (lno(),df)  
            return p_vol,b_vol,bp_vol_per,p_oi,b_oi,bs_oi_per,p_sub_b,p_add_b
            #print(lno(),total_int)

        
    else :
        return 0,0,0,0,0,0,0,0
def get_op_ratio_df():
    """
    總PC 多方 110 以上  130以上嘎空籌碼  150以上極端
    總PC 空方 100以下
    月PC 125 以上多方  160極端值
    月PC 100以下空方
    周PC 120以上多方  180以上極值
    周PC 100以下空方  70以下極值
    總 P-C > 10萬  嘎空籌碼
    """
    out_file='csv/op/op_data.csv'
    #print(lno(),date)
    if os.path.exists(out_file): 
        
        #date_str='%d/%02d/%02d'%(int(date.year), int(date.month),int(date.day))
        df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype={'日期': 'str'})
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        df_s['賣+買']=df_s['賣權未平倉量']+df_s['買權未平倉量']
        df_s['賣-買']=df_s['賣權未平倉量']-df_s['買權未平倉量']
        return df_s
    else :
        return  pd.DataFrame() 
        
def get_opData(date,debug=0):


    nowdate=date
    tmp_list=[]
    day=0
    cnt=0
    while   len(tmp_list)<2 :
        nowdate = date - relativedelta(days=day)
        #print(lno(),nowdate)
        res=[]
        res=get_Op_Data_df_list(nowdate) 
        if res!=[]:
            tmp_list.append(res)
            #print(lno(),res)
            cnt=cnt+1
        if cnt>=5:
            break
        day=day+1
    if len(tmp_list)==2:
        df=tmp_list[0][1]
        df_prev=tmp_list[1][1]
        df['diff']=0
        df.reset_index(inplace=True)
        for i in range (0,len(df)):
            if debug==1:
                print (lno(),df.at[i,'買賣權'],df.at[i,'履約價'])
            df_pp=df_prev[(df_prev['買賣權'] == df.at[i,'買賣權'])&(df_prev['履約價']==df.at[i,'履約價'])].reset_index()
            if len(df_pp)==1:
                df.at[i,'diff']=int(df.at[i,'未沖銷契約數'])-int(df_pp.at[0,'未沖銷契約數'])
                #print(lno(),df.at[i,'履約價'],df.at[i,'diff'],df.at[i,'未沖銷契約數'],df_pp.at[0,'未沖銷契約數'])
            else :
                df.at[i,'diff']=df.at[i,'未沖銷契約數']
        
        df_b1=df[(df['買賣權']=='買權')].sort_values('未沖銷契約數', ascending=False).head(2).reset_index()
        df_b2=df[(df['買賣權']=='買權')].sort_values('diff', ascending=False).head(2).reset_index()
        print(lno(),df_b1,df_b2)
        b1=[]
        for i in range(0,2):
            b1.append([df_b1.at[i,'履約價'],df_b1.at[i,'未沖銷契約數'],df_b1.at[i,'diff']])

        for i in range(0,2):
            b1.append([df_b2.at[i,'履約價'],df_b2.at[i,'未沖銷契約數'],df_b2.at[i,'diff']])
        #print(lno(),b1)
        #print(lno(),df_b2)
        df_s1=df[(df['買賣權']=='賣權')].sort_values('未沖銷契約數', ascending=False).head(2).reset_index()
        df_s2=df[(df['買賣權']=='賣權')].sort_values('diff', ascending=False).head(2).reset_index()
        s1=[]
        for i in range(0,2):
            s1.append([df_s1.at[i,'履約價'],df_s1.at[i,'未沖銷契約數'],df_s1.at[i,'diff']])
        for i in range(0,2):
            s1.append([df_s2.at[i,'履約價'],df_s2.at[i,'未沖銷契約數'],df_s2.at[i,'diff']])
        #print(lno(),b1+s1)
        #print(lno(),df_s1,df_s2)
        #df_b.reset_index(inplace=True)
        #print (lno(),df_b)
        #df_s=df[(df['交易時段'] == '一般')&(df['買賣權']=='賣權')]
        
        #df_s.reset_index(inplace=True)
        
        #total=float(df.iat[0,'外資total'].strip().replace(',', ''))
        
        return b1+s1
        
    else :
        return []   

def get_opData_org(date,debug=0):
    out_file='csv/op/optData_%d%02d%02d.csv'%(int(date.year), int(date.month),int(date.day))
    #print(lno(),date)
    if os.path.exists(out_file): 
        month_date_str='%d%02d'%(int(date.year), int(date.month))
        next_mon = date + relativedelta(months=1)
        next_month_date_str='%d%02d'%(int(next_mon.year), int(next_mon.month))
        df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype={'到期月份(週別)': 'str'})
        df_s=df_s[(df_s['交易時段'] == '一般')]
        df_s['到期月份(週別)']=[x.strip() for x in df_s['到期月份(週別)'] ]
        df_s['未沖銷契約數']=df_s['未沖銷契約數'].astype('int')
        df_s['履約價']=df_s['履約價'].astype('int')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print (lno(),df_s['到期月份(週別)'].dtypes,month_date_str,next_month_date_str)
        #print(lno(),df_s[(df_s['到期月份(週別)'] == next_month_date_str)].values.tolist())
        index_date_str=month_date_str
        print(lno(),month_date_str)
        
        week1_str='%sW1'%(month_date_str)
        week2_str='%sW2'%(month_date_str)
        week4_str='%sW4'%(month_date_str)
        week4_str='%sW5'%(month_date_str)
        df=df_s[(df_s['到期月份(週別)'] == month_date_str)]
        df_next_moth=df_s[(df_s['到期月份(週別)'] == next_month_date_str)]
        df_w1=df_s[(df_s['到期月份(週別)'] == month_date_str+'W1')]
        df_w2=df_s[(df_s['到期月份(週別)'] == month_date_str+'W2')]
        df_w4=df_s[(df_s['到期月份(週別)'] == month_date_str+'W4')]
        df_w5=df_s[(df_s['到期月份(週別)'] == month_date_str+'W5')]
        if len(df_w1)!=0 :
            if len(df_w2)!=0:
                df_w=df_w2
            else :
                df_w=df_w1
                
        elif len(df_w2)!=0: 
            df_w=df_w2
        elif len(df_w4)!=0: 
            if len(df_w5)!=0:
                df_w=df_w5
            else :
                df_w=df_w4
            index_date_str=next_month_date_str       
        elif len(df_w5)!=0: 
            df_w=df_w5
            index_date_str=next_month_date_str   
        else :    
            df_w=df
            
  
        df=df[(df['交易時段'] == '一般')]
  
        day=1
        while   day<=30 :
            prev_date = date - relativedelta(days=day)
            prev_out_file='csv/op/optData_%d%02d%02d.csv'%(int(prev_date.year), int(prev_date.month),int(prev_date.day))
            if debug==1:
                print(lno(),prev_out_file)
            if os.path.exists(prev_out_file): 
                df_prev = pd.read_csv(prev_out_file,encoding = 'utf-8',dtype={'到期月份(週別)': 'str'})
                df_prev=df_prev[(df_prev['交易時段'] == '一般')]
                try:
                    df_prev['到期月份(週別)']=[x.strip() for x in df_prev['到期月份(週別)'] ]
                except:
                    print(lno(),prev_out_file,df_prev['到期月份(週別)'])
                    continue
                df_prev=df_prev[(df_prev['到期月份(週別)'] == index_date_str)]
                df_prev['未沖銷契約數']=df_prev['未沖銷契約數'].astype('int')
                df_prev['履約價']=df_prev['履約價'].astype('int')
                if debug==1:
                    print(lno(),df_prev.tail())
                break
            day=day+1
        #print(lno(),df_prev.dtypes) 
        df['diff']=0
        df.reset_index(inplace=True)
        for i in range (0,len(df)):
            if debug==1:
                print (lno(),df.at[i,'買賣權'],df.at[i,'履約價'])
            df_pp=df_prev[(df_prev['買賣權'] == df.at[i,'買賣權'])&(df_prev['履約價']==df.at[i,'履約價'])].reset_index()
            if len(df_pp)==1:
                df.at[i,'diff']=int(df.at[i,'未沖銷契約數'])-int(df_pp.at[0,'未沖銷契約數'])
                #print(lno(),df.at[i,'履約價'],df.at[i,'diff'],df.at[i,'未沖銷契約數'],df_pp.at[0,'未沖銷契約數'])
            else :
                df.at[i,'diff']=df.at[i,'未沖銷契約數']
                #print(lno(),df.at[i,'履約價'],df.at[i,'diff'],df.at[i,'未沖銷契約數'],df_pp.at[0,'未沖銷契約數'])
        print(lno(),df.tail())    
            
        
        df_b1=df[(df['買賣權']=='買權')].sort_values('未沖銷契約數', ascending=False).head(2).reset_index()
        df_b2=df[(df['買賣權']=='買權')].sort_values('diff', ascending=False).head(2).reset_index()
        print(lno(),df_b1,df_b2)
        b1=[]
        for i in range(0,2):
            b1.append([df_b1.at[i,'履約價'],df_b1.at[i,'未沖銷契約數'],df_b1.at[i,'diff']])

        for i in range(0,2):
            b1.append([df_b2.at[i,'履約價'],df_b2.at[i,'未沖銷契約數'],df_b2.at[i,'diff']])
        #print(lno(),b1)
        #print(lno(),df_b2)
        df_s1=df[(df['買賣權']=='賣權')].sort_values('未沖銷契約數', ascending=False).head(2).reset_index()
        df_s2=df[(df['買賣權']=='賣權')].sort_values('diff', ascending=False).head(2).reset_index()
        s1=[]
        for i in range(0,2):
            s1.append([df_s1.at[i,'履約價'],df_s1.at[i,'未沖銷契約數'],df_s1.at[i,'diff']])
        for i in range(0,2):
            s1.append([df_s2.at[i,'履約價'],df_s2.at[i,'未沖銷契約數'],df_s2.at[i,'diff']])
        #print(lno(),b1+s1)
        #print(lno(),df_s1,df_s2)
        #df_b.reset_index(inplace=True)
        #print (lno(),df_b)
        #df_s=df[(df['交易時段'] == '一般')&(df['買賣權']=='賣權')]
        
        #df_s.reset_index(inplace=True)
        
        #total=float(df.iat[0,'外資total'].strip().replace(',', ''))
        
        return b1+s1
        
    else :
        return []   
        
def week_of_month(dt):
    """ Returns the week of the month for the specified date.
    """
    first_day = dt.replace(day=1)
    dom = dt.day
    adjusted_dom = dom + first_day.weekday()
    if first_day.weekday()>=2:
        if dt.weekday()<2:
            return int(ceil(adjusted_dom/7.0)) -1
        return int(ceil(adjusted_dom/7.0)) 
    if dt.weekday()>=2:
        return (int(ceil(adjusted_dom/7.0))+1)
    return int(ceil(adjusted_dom/7.0))
    
def get_Op_Week_Month_Data(date,debug=0):
    out_file='csv/op/optData_%d%02d%02d.csv'%(int(date.year), int(date.month),int(date.day))
    #print(lno(),date)
    if os.path.exists(out_file): 
        month_date_str='%d%02d'%(int(date.year), int(date.month))
        next_mon = date + relativedelta(months=1)
        next_month_date_str='%d%02d'%(int(next_mon.year), int(next_mon.month))
        next_2mon = date + relativedelta(months=2)
        next_2month_date_str='%d%02d'%(int(next_2mon.year), int(next_2mon.month))
        df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype={'到期月份(週別)': 'str'})
        df_s=df_s[(df_s['交易時段'] == '一般')]
        df_s['到期月份(週別)']=[x.strip() for x in df_s['到期月份(週別)'] ]
        df_s['未沖銷契約數']=df_s['未沖銷契約數'].astype('int')
        df_s['履約價']=df_s['履約價'].astype('int')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print (lno(),df_s['到期月份(週別)'].dtypes,month_date_str,next_month_date_str)
        #print(lno(),df_s[(df_s['到期月份(週別)'] == next_month_date_str)].values.tolist())
        
        #print(lno(),date.year,date.month,date.day,week_of_month(date),date.weekday())
        """
        if week_of_month(date)==1:
            week_str='%sW%d'%(month_date_str,week_of_month(date))
            month_str=month_date_str
        elif week_of_month(date)==2:
            week_str='%sW%d'%(month_date_str,week_of_month(date))
            month_str=month_date_str
        elif week_of_month(date)==3:
            week_str=month_date_str
            month_str=month_date_str
        elif week_of_month(date)==4:
            week_str='%sW%d'%(month_date_str,week_of_month(date))
            month_str=next_month_date_str
        elif week_of_month(date)==5:
            if date.weekday()<2:
                add_day=2- date.weekday()
            else:
                add_day=7+2-date.weekday()
            next3day=date+relativedelta(days=add_day)
            if next3day.month!=date.month:
                week_str='%sW1'%(next_month_date_str)
            else :
                week_str='%sW%d'%(month_date_str,week_of_month(date))
            month_str=next_month_date_str
        elif week_of_month(date)==6:
            week_str='%sW1'%(next_month_date_str)
            month_str=next_month_date_str    
        else:
            print(lno(),"err",date)
        
        df_m=df_s[(df_s['到期月份(週別)'] == month_str)]
        df_w=df_s[(df_s['到期月份(週別)'] == week_str)]
        print(lno(),"月",week_of_month(date),date.weekday(),df_m[['交易日期','到期月份(週別)']].tail(1).values)
        print(lno(),"周",week_of_month(date),date.weekday(),df_w[['交易日期','到期月份(週別)']].tail(1).values)
        """
        """
        邏輯:
        第一周:月跟W1 無W2
        第2周:月跟W2 無W4
        第3周:月  無W4
        第4周:下月 W4 無W5
        第5周:下月 W5
        """
        
        week1_str='%sW1'%(month_date_str)
        week2_str='%sW2'%(month_date_str)
        week4_str='%sW4'%(month_date_str)
        week4_str='%sW5'%(month_date_str)
        df_m=df_s[(df_s['到期月份(週別)'] == month_date_str)]
        df_next_moth=df_s[(df_s['到期月份(週別)'] == next_month_date_str)]
        df_w1=df_s[(df_s['到期月份(週別)'] == month_date_str+'W1')]
        df_w2=df_s[(df_s['到期月份(週別)'] == month_date_str+'W2')]
        df_w4=df_s[(df_s['到期月份(週別)'] == month_date_str+'W4')]
        df_w5=df_s[(df_s['到期月份(週別)'] == month_date_str+'W5')]
        df_next_month_w1=df_s[(df_s['到期月份(週別)'] == next_month_date_str+'W1')]
            
        
        if len(df_w1)!=0 :
            if len(df_w2)!=0:
                df_w=df_w2
                index_date_str=month_date_str+'W2'
                if debug==1:
                    print(lno(),date,month_date_str,"第2周")
            else :
                df_w=df_w1
                index_date_str=month_date_str+'W1'
                #print(lno(),date,index_date_str)
                if debug==1:
                    print(lno(),date,month_date_str,"第1周")
        elif len(df_w2)!=0:
            if date.weekday()==2:
                if debug==1:
                    print(lno(),date,month_date_str,"第3周")
                df_w=df_m
            else:    
                df_w=df_w2
                index_date_str=month_date_str+'W2'
                if debug==1:
                    print(lno(),date,month_date_str,"第2周")
        elif len(df_w4)!=0: 
            if len(df_w5)!=0:
                df_w=df_w5
                df_m=df_next_moth
                df_next_moth=df_s[(df_s['到期月份(週別)'] == next_2month_date_str)]
                if debug==1:
                    print(lno(),date,"第5周")
            elif len(df_next_month_w1)!=0:
                if debug==1:
                    print(lno(),date,"下月第1周")
                df_w=df_next_month_w1
                df_m=df_next_moth
                df_next_moth=df_s[(df_s['到期月份(週別)'] == next_2month_date_str)]        
            else :
                if debug==1:
                    print(lno(),date,"第4周")
                df_w=df_w4
                df_m=df_next_moth
                df_next_moth=df_s[(df_s['到期月份(週別)'] == next_2month_date_str)]

        elif len(df_w5)!=0:
            if len(df_next_month_w1)!=0:
                if debug==1:
                    print(lno(),date,"下月第1周")
                df_w=df_next_month_w1
                df_m=df_next_moth
                df_next_moth=df_s[(df_s['到期月份(週別)'] == next_2month_date_str)]
            else:
                if debug==1:
                    print(lno(),date,"第5周")
                df_w=df_w5
                df_m=df_next_moth
                df_next_moth=df_s[(df_s['到期月份(週別)'] == next_2month_date_str)]
        elif len(df_next_month_w1)!=0:
                if debug==1:
                    print(lno(),date,"下月第1周")
                df_w=df_next_month_w1
                df_m=df_next_moth
                df_next_moth=df_s[(df_s['到期月份(週別)'] == next_2month_date_str)]
        else :    
            if debug==1:
                print(lno(),date,"第3周")
            df_w=df_m
            index_date_str=month_date_str
        if debug==1:    
            print(lno(),"周",week_of_month(date),date.weekday(),df_w[['交易日期','到期月份(週別)']].tail(1).values)
            print(lno(),"月",week_of_month(date),date.weekday(),df_m[['交易日期','到期月份(週別)']].tail(1).values)
            print(lno(),"次",week_of_month(date),date.weekday(),df_next_moth[['交易日期','到期月份(週別)']].tail(1).values)
        #df_w['未沖銷契約數']=df_w['未沖銷契約數'].astype('int')
        df_w_call=df_w[(df_w['買賣權']=='買權')].copy()
        df_w_put=df_w[(df_w['買賣權']=='賣權')].copy()
        w_call_oi=df_w_call['未沖銷契約數'].astype('int').sum()
        w_put_oi=df_w_put['未沖銷契約數'].astype('int').sum()
        w_call_vol=df_w_call['成交量'].astype('int').sum()
        w_put_vol=df_w_put['成交量'].astype('int').sum()
        
        df_m_call=df_m[(df_m['買賣權']=='買權')].copy()
        df_m_put=df_m[(df_m['買賣權']=='賣權')].copy()
        m_call_oi=df_m_call['未沖銷契約數'].astype('int').sum()
        m_put_oi=df_m_put['未沖銷契約數'].astype('int').sum()
        m_call_vol=df_m_call['成交量'].astype('int').sum()
        m_put_vol=df_m_put['成交量'].astype('int').sum()
        
        df_next_m_call=df_next_moth[(df_next_moth['買賣權']=='買權')].copy()
        df_next_m_put=df_next_moth[(df_next_moth['買賣權']=='賣權')].copy()
        next_m_call_oi=df_next_m_call['未沖銷契約數'].astype('int').sum()
        next_m_put_oi=df_next_m_put['未沖銷契約數'].astype('int').sum()
        next_m_call_vol=df_next_m_call['成交量'].astype('int').sum()
        next_m_put_vol=df_next_m_put['成交量'].astype('int').sum()
        #if debug==1:    
        
        print(lno(),'Week',w_put_oi,w_call_oi,w_call_vol,m_put_vol)
        print(lno(),'   M',m_put_oi,m_call_oi,m_call_vol,m_put_vol)
        print(lno(),'  2M',next_m_put_oi,next_m_call_oi,next_m_call_vol,next_m_put_vol)
        return '%d-%02d-%02d'%(int(date.year), int(date.month),int(date.day)),m_put_oi,m_call_oi,m_put_vol,m_call_vol,next_m_put_oi,next_m_call_oi,next_m_put_vol,next_m_call_vol,w_put_oi,w_call_oi,w_put_vol,w_call_vol
    return []
def get_week_month_op_df(startdate,enddate):
    #startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
    day=0
    nowdate=enddate
    tmp_list=[]
    while   nowdate>=startdate :
        nowdate = enddate - relativedelta(days=day)
        #print(lno(),nowdate)
        res=[]
        res=get_Op_Week_Month_Data(nowdate) 
        if res!=[]:
            tmp_list.append(res)
            print(lno(),res)
        day=day+1
    labels =['日期','月P_OI','月C_OI','月P_VOL','月C_VOL','次月P_OI','次月C_OI','次月P_VOL','次月C_VOL','周P_OI','周C_OI','周P_VOL','周C_VOL']
    df = pd.DataFrame.from_records(tmp_list, columns=labels)
    df['月PC_OI_RATIO']=df['月P_OI']/df['月C_OI']*100
    df['月PC_VOL_RATIO']=df['月P_VOL']/df['月C_VOL']*100
    df['次月PC_OI_RATIO']=df['次月P_OI']/df['次月C_OI']*100
    df['次月PC_VOL_RATIO']=df['次月P_VOL']/df['次月C_VOL']*100
    df['周PC_OI_RATIO']=df['周P_OI']/df['周C_OI']*100
    df['周PC_VOL_RATIO']=df['周P_VOL']/df['周C_VOL']*100
    print (lno(),df)
    #print(lno(),df.round({'月PC': 1,'周PC': 1}))
    return df.round({'月PC_OI_RATIO': 1,'月PC_VOL_RATIO': 1,'次月PC_OI_RATIO': 1,'次月PC_VOL_RATIO': 1,'周PC_OI_RATIO': 1,'周PC_VOL_RATIO': 1})

def get_Op_Data_df_list(date,debug=1):
    out_file='csv/op/optData_%d%02d%02d.csv'%(int(date.year), int(date.month),int(date.day))
    #print(lno(),date)
    if os.path.exists(out_file): 
        month_date_str='%d%02d'%(int(date.year), int(date.month))
        next_mon = date + relativedelta(months=1)
        next_month_date_str='%d%02d'%(int(next_mon.year), int(next_mon.month))
        next_2mon = date + relativedelta(months=2)
        next_2month_date_str='%d%02d'%(int(next_2mon.year), int(next_2mon.month))
        df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype={'到期月份(週別)': 'str'})
        df_s=df_s[(df_s['交易時段'] == '一般')]
        df_s['到期月份(週別)']=[x.strip() for x in df_s['到期月份(週別)'] ]
        df_s['未沖銷契約數']=df_s['未沖銷契約數'].astype('int')
        df_s['履約價']=df_s['履約價'].astype('int')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print(lno(),date.year,date.month,date.day,week_of_month(date),date.weekday())

        """
        邏輯:
        第一周:月跟W1 無W2
        第2周:月跟W2 無W4
        第3周:月  無W4
        第4周:下月 W4 無W5
        第5周:下月 W5
        """
        
        week1_str='%sW1'%(month_date_str)
        week2_str='%sW2'%(month_date_str)
        week4_str='%sW4'%(month_date_str)
        week4_str='%sW5'%(month_date_str)
        df_m=df_s[(df_s['到期月份(週別)'] == month_date_str)].copy()
        df_next_moth=df_s[(df_s['到期月份(週別)'] == next_month_date_str)].copy()
        df_w1=df_s[(df_s['到期月份(週別)'] == month_date_str+'W1')]
        df_w2=df_s[(df_s['到期月份(週別)'] == month_date_str+'W2')]
        df_w4=df_s[(df_s['到期月份(週別)'] == month_date_str+'W4')]
        df_w5=df_s[(df_s['到期月份(週別)'] == month_date_str+'W5')]
        df_next_month_w1=df_s[(df_s['到期月份(週別)'] == next_month_date_str+'W1')]
            
        
        if len(df_w1)!=0 :
            if len(df_w2)!=0:
                df_w=df_w2
                index_date_str=month_date_str+'W2'
                if debug==1:
                    print(lno(),date,month_date_str,"第2周")
            else :
                df_w=df_w1
                index_date_str=month_date_str+'W1'
                #print(lno(),date,index_date_str)
                if debug==1:
                    print(lno(),date,month_date_str,"第1周")
        elif len(df_w2)!=0:
            if date.weekday()==2:
                if debug==1:
                    print(lno(),date,month_date_str,"第3周")
                df_w=df_m
            else:    
                df_w=df_w2
                index_date_str=month_date_str+'W2'
                if debug==1:
                    print(lno(),date,month_date_str,"第2周")
        elif len(df_w4)!=0: 
            if len(df_w5)!=0:
                df_w=df_w5
                df_m=df_next_moth
                df_next_moth=df_s[(df_s['到期月份(週別)'] == next_2month_date_str)]
                if debug==1:
                    print(lno(),date,"第5周")
            elif len(df_next_month_w1)!=0:
                if debug==1:
                    print(lno(),date,"下月第1周")
                df_w=df_next_month_w1
                df_m=df_next_moth
                df_next_moth=df_s[(df_s['到期月份(週別)'] == next_2month_date_str)]          
            else :
                if debug==1:
                    print(lno(),date,"第4周")
                df_w=df_w4
                df_m=df_next_moth
                df_next_moth=df_s[(df_s['到期月份(週別)'] == next_2month_date_str)]

        elif len(df_w5)!=0:
            if len(df_next_month_w1)!=0:
                if debug==1:
                    print(lno(),date,"下月第1周")
                df_w=df_next_month_w1
                df_m=df_next_moth
                df_next_moth=df_s[(df_s['到期月份(週別)'] == next_2month_date_str)]
            else:
                if debug==1:
                    print(lno(),date,"第5周")
                df_w=df_w5
                df_m=df_next_moth
                df_next_moth=df_s[(df_s['到期月份(週別)'] == next_2month_date_str)]
        elif len(df_next_month_w1)!=0:
                if debug==1:
                    print(lno(),date,"下月第1周")
                df_w=df_next_month_w1
                df_m=df_next_moth
                df_next_moth=df_s[(df_s['到期月份(週別)'] == next_2month_date_str)]
        else :    
            if debug==1:
                print(lno(),date,"第3周")
            df_w=df_m
            index_date_str=month_date_str
        if debug==1:    
            print(lno(),"周",week_of_month(date),date.weekday(),df_w[['交易日期','到期月份(週別)']].tail(1).values)
            print(lno(),"月",week_of_month(date),date.weekday(),df_m[['交易日期','到期月份(週別)']].tail(1).values)
            print(lno(),"次",week_of_month(date),date.weekday(),df_next_moth[['交易日期','到期月份(週別)']].tail(1).values)

        return [df_w,df_m,df_next_moth]
    return []    
def get_OptData_dfs_list(date,debug=1):
    out_file='csv/op/optData_%d%02d%02d.csv'%(int(date.year), int(date.month),int(date.day))
    #print(lno(),date)
    if os.path.exists(out_file): 
        df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype={'到期月份(週別)': 'str'})
        df_s=df_s[(df_s['交易時段'] == '一般')]
        df_s['到期月份(週別)']=[x.strip() for x in df_s['到期月份(週別)'] ]
        df_s['未沖銷契約數']=df_s['未沖銷契約數'].astype('int')
        df_s['履約價']=df_s['履約價'].astype('int')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        
        #print(lno(),df_s.at[0,'到期月份(週別)'])
        list=[]
        dfs=df_s.copy()
        while True:
            if len(dfs)==0:
                break
            index_str=dfs.at[0,'到期月份(週別)']
            #print(lno(),index_str)
            df1=dfs[(dfs['到期月份(週別)'] == index_str)].copy()
            list.append(df1)
            dfs=dfs[(dfs['到期月份(週別)'] != index_str)].copy().reset_index(drop=True)
        
        return list
    return []    
def get_OptData_df(date,debug=1):
    out_file='csv/op/optData_%d%02d%02d.csv'%(int(date.year), int(date.month),int(date.day))
    #print(lno(),date)
    if os.path.exists(out_file): 
        df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype={'到期月份(週別)': 'str'})
        df_s=df_s[(df_s['交易時段'] == '一般')]
        df_s['到期月份(週別)']=[x.strip() for x in df_s['到期月份(週別)'] ]
        df_s['未沖銷契約數']=df_s['未沖銷契約數'].astype('int')
        df_s['履約價']=df_s['履約價'].astype('int')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        return df_s.reset_index(drop=True)
    return pd.DataFrame()    
def get_need_plot_df(df_org,df_prev_org,debug=0):
    df=df_org.copy()
    df_prev=df_prev_org.copy()
    df['diff']=0
    df.reset_index(inplace=True)
    for i in range (0,len(df)):
        if debug==2:
            print (lno(),df.at[i,'買賣權'],df.at[i,'履約價'])
        df_pp=df_prev[(df_prev['買賣權'] == df.at[i,'買賣權'])&(df_prev['履約價']==df.at[i,'履約價'])].reset_index()
        if len(df_pp)==1:
            df.at[i,'diff']=int(df.at[i,'未沖銷契約數'])-int(df_pp.at[0,'未沖銷契約數'])
            #print(lno(),df.at[i,'履約價'],df.at[i,'diff'],df.at[i,'未沖銷契約數'],df_pp.at[0,'未沖銷契約數'])
        else :
            df.at[i,'diff']=df.at[i,'未沖銷契約數']
    #df_call=df[(df['買賣權']=='買權')].copy() 
    df_call=df[(df['買賣權']=='買權')].copy()[['到期月份(週別)','履約價','買賣權','未沖銷契約數','diff']] 
    df_put=df[(df['買賣權']=='賣權')].copy()[['到期月份(週別)','履約價','買賣權','未沖銷契約數','diff']] 
    #print(lno(),df_call['未沖銷契約數'].max())
    df_call_max=df_call.sort_values('未沖銷契約數', ascending=False).head(1).reset_index()
    df_put_max=df_put.sort_values('未沖銷契約數', ascending=False).head(1).reset_index()
    center_price=(df_call_max.iloc[0]['履約價']+df_put_max.iloc[0]['履約價'])/2
    low_price=center_price-500
    high_price=center_price+500
    print(lno(),center_price)

    #df_call1=df_call[(df_call.loc[:,"履約價"] <= high_price) & (df_call.loc[:,"履約價"] >= low_price)]
    df_call=df_call[(df_call.loc[:,"履約價"] <= high_price) & (df_call.loc[:,"履約價"] >= low_price)].reset_index()
    #print(lno(),df_call)
    df_put=df_put[(df_put.loc[:,"履約價"] <= high_price) & (df_put.loc[:,"履約價"] >= low_price)].reset_index()

    return df_call,df_put
def op_plot(date,debug=0):
    nowdate=date
    tmp_list=[]
    day=0
    cnt=0
    while   len(tmp_list)<2 :
        nowdate = date - relativedelta(days=day)
        #print(lno(),nowdate)
        res=[]
        res=get_Op_Data_df_list(nowdate) 
        if res!=[]:
            tmp_list.append(res)
            #print(lno(),res)
            cnt=cnt+1
        if cnt>=5:
            break
        day=day+1
    if len(tmp_list)==2:
        

        matplotlib.rcParams['axes.unicode_minus']=False 
        #plt.figure()
        fig, axes = plt.subplots(nrows=3, ncols=2)
        fig.tight_layout()
        plt.subplot(3,2,1)
        df_call,df_put=get_need_plot_df(tmp_list[0][0],tmp_list[1][0])
        print(lno(),df_call.index)
        #index=np.arange(0,len(df_call['履約價']))
        index=df_call.index
        plt.barh(index, df_call['未沖銷契約數'], color ='r',label='call',alpha=0.6)
        plt.barh(index, -df_put['未沖銷契約數'], color ='g',label='put',alpha=0.6)
        print(lno(),type(df_call.iloc[0]['到期月份(週別)']))
        df_call_max=df_call.sort_values('未沖銷契約數', ascending=False).head(2)
        df_put_max=df_put.sort_values('未沖銷契約數', ascending=False).head(2)
        for i in range(0,len(df_call_max)) :
            plt.text(df_call_max.iloc[i]['未沖銷契約數'],df_call_max.index.tolist()[i],df_call_max.iloc[i]['履約價'],fontsize=12,ha='left',va='center')
        for i in range(0,len(df_put_max)) :
            plt.text(-df_put_max.iloc[i]['未沖銷契約數'],df_put_max.index.tolist()[i],df_put_max.iloc[i]['履約價'],fontsize=12,ha='right',va='center')    
        plt.title("%s OI"%(df_call.iloc[0]['到期月份(週別)']))
        plt.yticks(index,df_call['履約價'].values.tolist())
        #plt.xticks(np.arange(len(df_call['履約價'])),df_call['履約價'].values.tolist())
        plt.grid(True)
        plt.legend()
        
        plt.subplot(3,2,2)
        plt.barh(index, df_call['diff'], color ='r',label='call',alpha=0.6)
        plt.barh(index, -df_put['diff'], color ='g',label='put',alpha=0.6)
        df_call_max=df_call.sort_values('diff', ascending=False).head(2)
        df_put_max=df_put.sort_values('diff', ascending=False).head(2)
        for i in range(0,len(df_call_max)) :
            plt.text(df_call_max.iloc[i]['diff'],df_call_max.index.tolist()[i],df_call_max.iloc[i]['履約價'],fontsize=12,ha='left',va='center')
        for i in range(0,len(df_put_max)) :
            plt.text(-df_put_max.iloc[i]['diff'],df_put_max.index.tolist()[i],df_put_max.iloc[i]['履約價'],fontsize=12,ha='right',va='center')
        plt.yticks(index,df_call['履約價'].values.tolist())
        plt.title("%s OI diff "%(df_call.iloc[0]['到期月份(週別)']))
        plt.grid(True)
        plt.legend()
        
        df_call,df_put=get_need_plot_df(tmp_list[0][1],tmp_list[1][1])

        plt.subplot(3,2,3)
        #index=np.arange(0,len(df_call['履約價']))
        index=df_call.index
        plt.barh(index, df_call['未沖銷契約數'], color ='r',label='call',alpha=0.6)
        plt.barh(index, -df_put['未沖銷契約數'], color ='g',label='put',alpha=0.6)
        df_call_max=df_call.sort_values('未沖銷契約數', ascending=False).head(2)
        df_put_max=df_put.sort_values('未沖銷契約數', ascending=False).head(2)
        for i in range(0,len(df_call_max)) :
            plt.text(df_call_max.iloc[i]['未沖銷契約數'],df_call_max.index.tolist()[i],df_call_max.iloc[i]['履約價'],fontsize=12,ha='left',va='center')
        for i in range(0,len(df_put_max)) :
            plt.text(-df_put_max.iloc[i]['未沖銷契約數'],df_put_max.index.tolist()[i],df_put_max.iloc[i]['履約價'],fontsize=12,ha='right',va='center')    
        plt.yticks(index,df_call['履約價'].values.tolist())
        plt.title("%s OI "%(df_call.iloc[0]['到期月份(週別)']))
        plt.grid(True)
        plt.legend()
        
        plt.subplot(3,2,4)
        plt.barh(index, df_call['diff'], color ='r',label='call',alpha=0.6)
        plt.barh(index, -df_put['diff'], color ='g',label='put',alpha=0.6)
        df_call_max=df_call.sort_values('diff', ascending=False).head(2)
        df_put_max=df_put.sort_values('diff', ascending=False).head(2)
        for i in range(0,len(df_call_max)) :
            plt.text(df_call_max.iloc[i]['diff'],df_call_max.index.tolist()[i],df_call_max.iloc[i]['履約價'],fontsize=12,ha='left',va='center')
        for i in range(0,len(df_put_max)) :
            plt.text(-df_put_max.iloc[i]['diff'],df_put_max.index.tolist()[i],df_put_max.iloc[i]['履約價'],fontsize=12,ha='right',va='center')
        plt.yticks(index,df_call['履約價'].values.tolist())
        plt.title("%s OI diff "%(df_call.iloc[0]['到期月份(週別)']))
        plt.grid(True)
        plt.legend()
        
        df_call,df_put=get_need_plot_df(tmp_list[0][2],tmp_list[1][2])

        plt.subplot(3,2,5)
        #index=np.arange(0,len(df_call['履約價']))
        index=df_call.index
        plt.barh(index, df_call['未沖銷契約數'], color ='r',label='call',alpha=0.6)
        plt.barh(index, -df_put['未沖銷契約數'], color ='g',label='put',alpha=0.6)
        df_call_max=df_call.sort_values('未沖銷契約數', ascending=False).head(2)
        df_put_max=df_put.sort_values('未沖銷契約數', ascending=False).head(2)
        for i in range(0,len(df_call_max)) :
            plt.text(df_call_max.iloc[i]['未沖銷契約數'],df_call_max.index.tolist()[i],df_call_max.iloc[i]['履約價'],fontsize=12,ha='left',va='center')
        for i in range(0,len(df_put_max)) :
            plt.text(-df_put_max.iloc[i]['未沖銷契約數'],df_put_max.index.tolist()[i],df_put_max.iloc[i]['履約價'],fontsize=12,ha='right',va='center')    
        plt.yticks(index,df_call['履約價'].values.tolist())
        plt.title("%s OI "%(df_call.iloc[0]['到期月份(週別)']))
        plt.grid(True)
        plt.legend()
        
        plt.subplot(3,2,6)
        plt.barh(index, df_call['diff'], color ='r',label='call',alpha=0.6)
        plt.barh(index, -df_put['diff'], color ='g',label='put',alpha=0.6)
        df_call_max=df_call.sort_values('diff', ascending=False).head(2)
        df_put_max=df_put.sort_values('diff', ascending=False).head(2)
        for i in range(0,len(df_call_max)) :
            plt.text(df_call_max.iloc[i]['diff'],df_call_max.index.tolist()[i],df_call_max.iloc[i]['履約價'],fontsize=12,ha='left',va='center')
        for i in range(0,len(df_put_max)) :
            plt.text(-df_put_max.iloc[i]['diff'],df_put_max.index.tolist()[i],df_put_max.iloc[i]['履約價'],fontsize=12,ha='right',va='center')
        plt.yticks(index,df_call['履約價'].values.tolist())
        plt.title("%s OI diff "%(df_call.iloc[0]['到期月份(週別)']))
        plt.grid(True)
        plt.legend()
        
        #plt.show()
        #fig = matplotlib.pyplot.gcf()
        fig.set_size_inches(18.5, 10.5)
        filen='day_report/%d%02d/op_%d%02d%02d.png'%(date.year,date.month,date.year,date.month,date.day)
        fig.savefig(filen, dpi=100)
        plt.clf()
        plt.close(fig)
        print(lno(),df_call)
def twdate2datetime64(x):
    tmp_list=x.split ('/')
    try :
        tmp_str="%d%0s%0s"%(int(tmp_list[0])+1911,tmp_list[1],tmp_list[2])
        fin=datetime.strptime(tmp_str,'%Y%m%d')
    except :
    #print tmp_list[0]
        print (lno(),tmp_list[0])
        pass
    return np.datetime64(fin)
def date_sub2time64(x):
    fin=datetime.strptime(x,'%Y-%m-%d')
    return np.datetime64(fin) 
def date_slash2time64(x):
    fin=datetime.strptime(x,'%Y/%m/%d')
    return np.datetime64(fin)   
    
TWIIPATH='csv/twii'
def generate_twii(startdate,enddate,debug=1):
    out_file='csv/twii/twii_data.csv'
    dst_folder='csv/twii'
    check_dst_folder(dst_folder)
    nowdatetime = enddate
    month=0
    while   nowdatetime>=startdate :
        print (lno(),nowdatetime)
        dstpath='%s/%d%02d'%(TWIIPATH,int(nowdatetime.year), int(nowdatetime.month))
        if os.path.exists(dstpath):   
            df = pd.read_csv(dstpath,encoding = 'big5',skiprows=1)
            df.dropna(axis=1,how='all',inplace=True)
            df.dropna(inplace=True)
            df['日期']=[twdate2datetime64(x) for x in df['日期'] ]
            df['成交股數']=[int(x.strip().replace(',', '')) for x in df['成交股數'] ]
            df['成交金額']=[int(x.strip().replace(',', '')) for x in df['成交金額'] ]
            df['發行量加權股價指數']=[float(x.strip().replace(',', '')) for x in df['發行量加權股價指數'] ]
            if debug==1:
                print(lno(),len(df),df)

            if os.path.exists(out_file): 
                df_s = pd.read_csv(out_file,encoding = 'utf-8')
                df_s.dropna(axis=1,how='all',inplace=True)
                df_s.dropna(inplace=True)
                if debug==1:
                    print(lno(),df_s.dtypes)
                df_s['日期']=[date_sub2time64(x) for x in df_s['日期'] ]    
                df_s=df_s.append(df,ignore_index=True)
                
                df_s.drop_duplicates(subset=['日期'],keep='first',inplace=True)
                if debug==1:
                    print(lno(),df_s.dtypes)
               
                df_s=df_s.sort_values(by=['日期'], ascending=False)
                df_s.to_csv(out_file,encoding='utf-8', index=False)
            else :
                df.to_csv(out_file,encoding='utf-8', index=False)    

        month=month+1
        nowdatetime = enddate - relativedelta(months=month)
        
def download_twii(startdate,enddate):
    check_dst_folder(TWIIPATH)
    nowdatetime = enddate
    month=0
    while   nowdatetime>=startdate :
        print (lno(),nowdatetime)
        filename='%s/%d%02d'%(TWIIPATH,int(nowdatetime.year), int(nowdatetime.month))
        url='http://www.twse.com.tw/exchangeReport/FMTQIK?response=csv&date=%d%02d%02d'%(int(nowdatetime.year),int(nowdatetime.month),int(nowdatetime.day))
        response = requests.get(url)
        # Check if the response is ok (200)
        if response.status_code == 200:
        # Open file and write the content
            with open(filename, 'wb') as file:
                # A chunk of 128 bytes
                for chunk in response:
                    file.write(chunk)
        time.sleep(3)
        month=month+1
        nowdatetime = enddate - relativedelta(months=month)
    generate_twii(startdate,enddate)    
def down_futData(startdate,enddate):
    dst_folder='csv/fut'
    filename='csv/fut/futtmp.csv'
    
    check_dst_folder(dst_folder)
    startdate_str='%d/%02d/%02d'%(int(startdate.year), int(startdate.month),int(startdate.day))
    enddate_str='%d/%02d/%02d'%(int(enddate.year), int(enddate.month),int(enddate.day))
    #url = 'http://www.taifex.com.tw/cht/3/optDailyMarketReport'
    url = 'https://www.taifex.com.tw/cht/3/futDataDown '

    query_params = {
        'down_type': '1',
        'commodity_id':'TX',
        'commodity_id2':'',
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
    
    try:
        df = pd.read_csv(filename,encoding = 'big5',skiprows=1,header=None)        
    except :
        print(lno(),startdate_str,enddate_str)
        return
    #df = pd.read_csv(filename,encoding = 'utf-8',skiprows=1,header=None)
    """
    df.columns=['交易日期','契約','到期月份(週別)','履約價','買賣權','開盤價','最高價','最低價','收盤價','成交量','結算價','未沖銷契約數','最後最佳買價','最後最佳賣價','歷史最高價','歷史最低價','是否因訊息面暫停交易','交易時段','dummy']
    #print (lno(),df.index)
    """
    df.dropna(axis=1,how='all',inplace=True)
    df.dropna(inplace=True)
    #df['交易日期']=[x.replace('  T', '').strip() for x in df['交易日期'] ]
    #df.reset_index(inplace=True)
    #df['日期'] =  pd.to_datetime(df['日期'], format='%Y/%m/%d')
    #print (lno(),df.iloc[0]['交易日期'],df['交易日期'].dtypes)
    nowdatetime = enddate
    day=0
    while   nowdatetime>=startdate :
        print (lno(),nowdatetime,startdate)
        index_date='%d/%02d/%02d'%(int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
        #print (lno(),index_date)
        out_file='csv/fut/futData_%d%02d%02d.csv'%(int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
        df_now=df[(df['交易日期'] == index_date)]
        if len(df_now)>0:
            #print(lno(),len(df_now.columns))
            df_now.to_csv(out_file,encoding='utf-8', index=False)
        day=day+1 
        nowdatetime = enddate - relativedelta(days=day)
    

def get_twii_df():

    out_file='csv/twii/twii_data.csv'
    df = pd.read_csv(out_file,encoding = 'utf-8')
    df.dropna(axis=1,how='all',inplace=True)
    df.dropna(inplace=True)
    df['日期']=[date_sub2time64(x) for x in df['日期'] ] 
    #df['成交股數']=[int(x.strip().replace(',', '')) for x in df['成交股數'] ]
    #df['成交金額']=[int(x.strip().replace(',', '')) for x in df['成交金額'] ]
    #df['發行量加權股價指數']=[float(x.strip().replace(',', '')) for x in df['發行量加權股價指數'] ]    
    #df['漲跌點數']=[float(x.strip().replace(',', '')) for x in df['漲跌點數'] ]    
    df.columns=['日期','成交股數','成交金額','成交筆數','指數','漲跌點數']
    df=df.sort_values(by=['日期'], ascending=False)
    return df
def gen_op_fin(date):
    df1=get_twii_df()
    df=df1[(df1.loc[:,"日期"] <= np.datetime64(date))]
    df=df.head(20)
    print(lno(),df)
    df1=df[['日期','指數']].copy()
    df_op=get_op_ratio_df()
    df_op['日期']=[date_slash2time64(x) for x in df_op['日期'] ]
    df1=pd.merge(df1,df_op)
    df1.columns=['日期','指數','P_VOL','C_VOL','PC_V_RATIO','P_OI','C_OI','PC_OI_RATIO','P+C','P-C']
    df_op2=get_week_month_op_df(df1.iloc[-1]['日期'],df1.iloc[0]['日期'])
    df_op2['日期']=[date_sub2time64(x) for x in df_op2['日期'] ]
    df1=pd.merge(df1,df_op2)
    df2=df1[['日期','PC_OI_RATIO','PC_V_RATIO']].copy()
    #df2['加']=df1['PC_OI_RATIO']+df1['PC_V_RATIO']
    df2['月OI_RATIO']=df1['月PC_OI_RATIO']
    #df2['月VOL_RATIO']=df1['月PC_VOL_RATIO']
    #df2['月差']=df1['月PC_OI_RATIO']-df1['月PC_VOL_RATIO']
    #df2['月加']=df1['月PC_OI_RATIO']+df1['月PC_VOL_RATIO']
    df2['次月OI_RATIO']=df1['次月PC_OI_RATIO']
    #df2['次月VOL_RATIO']=df1['次月PC_VOL_RATIO']
    #df2['次月加']=df1['次月PC_OI_RATIO']+df1['次月PC_VOL_RATIO']
    df2['指數']=df1['指數']
    df2['周OI_RATIO']=df1['周PC_OI_RATIO']
    #df2['周VOL_RATIO']=df1['周PC_VOL_RATIO']
    #df2['周差']=df1['周PC_OI_RATIO']-df1['周PC_VOL_RATIO']
    df2['漲跌點']=df2['指數']-df2['指數'].shift(-1)
    #print(lno(),df1)
    check_dst_folder('day_report/%d%02d'%(date.year,date.month))
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)
    filen='day_report/%d%02d/%d%02d%02d_選擇權_v1.html'%(date.year,date.month,date.year,date.month,date.day)
    df2.to_html(filen,escape=False,index=False,sparsify=True,border=2,index_names=False)
    pd.set_option('display.max_colwidth', old_width)  
    filen='day_report/%d%02d/%d%02d%02d_選擇權_v1.xls'%(date.year,date.month,date.year,date.month,date.day)
    df2.to_excel(filen,index=False)    
def gen_op_fin_v1(date):
    df1=get_twii_df()
    df=df1[(df1.loc[:,"日期"] <= np.datetime64(date))]
    df=df.head(20)
    print(lno(),df)
    df1=df[['日期','指數']].copy()
    df_op=get_op_ratio_df()
    df_op['日期']=[date_slash2time64(x) for x in df_op['日期'] ]
    df1=pd.merge(df1,df_op)
    df1.columns=['日期','指數','P_VOL','C_VOL','PC_V_RATIO','P_OI','C_OI','PC_OI_RATIO','P+C','P-C']
    df_op2=get_week_month_op_df(df1.iloc[-1]['日期'],df1.iloc[0]['日期'])
    df_op2['日期']=[date_sub2time64(x) for x in df_op2['日期'] ]
    df1=pd.merge(df1,df_op2)
    df2=df1[['日期','PC_OI_RATIO','PC_V_RATIO']].copy()
    #df2['加']=df1['PC_OI_RATIO']+df1['PC_V_RATIO']
    df2['月OI_RATIO']=df1['月PC_OI_RATIO']
    #df2['月VOL_RATIO']=df1['月PC_VOL_RATIO']
    #df2['月差']=df1['月PC_OI_RATIO']-df1['月PC_VOL_RATIO']
    #df2['月加']=df1['月PC_OI_RATIO']+df1['月PC_VOL_RATIO']
    df2['次月OI_RATIO']=df1['次月PC_OI_RATIO']
    #df2['次月VOL_RATIO']=df1['次月PC_VOL_RATIO']
    #df2['次月加']=df1['次月PC_OI_RATIO']+df1['次月PC_VOL_RATIO']
    df2['指數']=df1['指數']
    df2['周OI_RATIO']=df1['周PC_OI_RATIO']
    #df2['周VOL_RATIO']=df1['周PC_VOL_RATIO']
    #df2['周差']=df1['周PC_OI_RATIO']-df1['周PC_VOL_RATIO']
    df2['漲跌點']=df2['指數']-df2['指數'].shift(-1)
    #print(lno(),df1)
    check_dst_folder('day_report/%d%02d'%(date.year,date.month))
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)
    filen='day_report/%d%02d/%d%02d%02d_選擇權_v1.html'%(date.year,date.month,date.year,date.month,date.day)
    df2.to_html(filen,escape=False,index=False,sparsify=True,border=2,index_names=False)
    pd.set_option('display.max_colwidth', old_width)  
    filen='day_report/%d%02d/%d%02d%02d_選擇權_v1.xls'%(date.year,date.month,date.year,date.month,date.day)
    df2.to_excel(filen,index=False)       
def calc_op_2_fut_df(dd):
    d=dd.copy().reset_index(drop=True)
    #print(lno(),d)
    for i in range(len(d)):
        #print(lno(),type(date ))
        #print(lno(),type(pd.to_datetime(d.iloc[i]['交易日期'])) )
        d.at[i,'delta']=get_delta_ratio(datetime.strptime(d.iloc[i]['交易日期'],'%Y/%m/%d'),d.iloc[i]['買賣權'],d.iloc[i]['到期月份(週別)'],d.iloc[i]['履約價'])
        #print(lno(),d.at[i,'delta'])
    #print(lno(),d.tail())
    d=d.replace('-',0)    
    d['約當小台']=d['delta']*d['未沖銷契約數']
    return d
    #print(lno(),d)
def calc_op_pwr(dd):
    d=dd.copy().reset_index(drop=True)
    df_call=d[(d['買賣權']=='買權')].copy().reset_index(drop=True)
    df_call['強度']=0.0
    df_call['call小台']=0.0
    df_call['put小台']=0.0
    for i in range(len(df_call)):        
        dd=d[(d['買賣權']=='賣權') & (d['履約價']==df_call.iloc[i]['履約價']) ]['約當小台'].values.tolist()[0]
        #dd=d[ (d['履約價']==df_call.iloc[i]['履約價']) ]
        df_call.at[i,'call小台']=df_call.at[i,'約當小台']
        df_call.at[i,'put小台']=dd
        df_call.at[i,'強度']=df_call.at[i,'call小台']-df_call.at[i,'put小台']
        
        #get_delta_ratio(date,BoS,MoW,price)
    #print(lno(),df_call)  

    return df_call[['交易日期','契約','到期月份(週別)','履約價','強度','call小台','put小台']]
    
def get_final_OptData(startdate):
    op=get_OptData_dfs_list(startdate)
    op_now=get_OptData_df(startdate)
    
    if len(op_now)>0:
        now_date=startdate
        day=1
        while True:
            now_date = startdate - relativedelta(days=day)
            op_prev=get_OptData_df(now_date)
            if len(op_prev)!=0:
                break
            day=day+1
        #df['外資buy']=df['外資buy'].astype('float64')            
        #op=get_opWeek_Data(startdate)
        #print(lno(),op_prev.head())
        op_now['diff']=0
        op_now=op_now.replace('-',np.nan)
        op_prev=op_prev.replace('-',np.nan)
        #print(lno(),op_now.head())
        index_str=''
        _list=[]
        for i in range(0,len(op_now)):
            if index_str!=op_now.at[i,'到期月份(週別)']:
                index_str=op_now.at[i,'到期月份(週別)']
                _list.append(index_str)
            #print(lno(),op_now[['交易日期','契約','到期月份(週別)','履約價']])
            df=op_prev[(op_prev['到期月份(週別)'] == op_now.at[i,'到期月份(週別)'])&(op_prev['買賣權'] == op_now.at[i,'買賣權'])&(op_prev['履約價']==op_now.at[i,'履約價'])].reset_index(drop=True)
            if len(df)>0:
                op_now.at[i,'diff']=op_now.at[i,'未沖銷契約數']-df.at[0,'未沖銷契約數']
            else:
                op_now.at[i,'diff']=op_now.at[i,'未沖銷契約數']
            #if i>3 :
            #    break
            #print(lno(),op_now.at[i,'買賣權'],op_now.at[i,'履約價'])
            #print(lno(),df)
            #print(lno(),op_now.at[i,'未沖銷契約數'])
        #op_now=get_need_df(op_now)
        #"""
        if 'W1' in _list[0] and  'W2' in _list[1]:
            _list.remove(_list[0])
        elif 'W1' in _list[1] :
            _list.remove(_list[0])
        elif 'W2' in _list[0] and startdate.weekday()==2:
            _list.remove(_list[0])    
        elif 'W4' in _list[1] :
            _list.remove(_list[0])  
        elif 'W5' in _list[1] :
            del _list[0]    
        #"""
        new_list=_list[0:4]
        df_list=[]
        for i in new_list:
            df=op_now[(op_now['到期月份(週別)'] ==i)]
            #print(lno(),df.head())
            df_list.append(df)
        print(lno(),startdate,new_list)
        return df_list    
    return pd.DataFrame()

def op_plot_oi(df,outf):
    
    df_call=df[(df['買賣權']=='買權')].copy().reset_index(drop=True)
    df_put=df[(df['買賣權']=='賣權')].copy().reset_index(drop=True)
    #"""
    df_call_max=df_call.sort_values('未沖銷契約數', ascending=False).head(1).reset_index()
    df_put_max=df_put.sort_values('未沖銷契約數', ascending=False).head(1).reset_index()
    center_price=(df_call_max.iloc[0]['履約價']+df_put_max.iloc[0]['履約價'])/2
    center_pos=df_call[(df_call.loc[:,"履約價"] >= center_price)].index.values.tolist()[0]
    #print(lno(),df_call[(df_call.loc[:,"履約價"] >= center_price)].index.values.tolist()[0])
    
    start_pos=center_pos-12
    if start_pos<0:
        start_pos=0
    end_pos=center_pos+12
    if end_pos>len(df)-1:
        end_pos=len(df)-1
    df_call=df_call.loc[start_pos:end_pos].copy().reset_index(drop=True)  
    df_put=df_put.loc[start_pos:end_pos].copy().reset_index(drop=True)    
    #print(lno(),df_call)
    #print(lno(),df_call.iloc[0]['履約價'],df_call.iloc[-1]['履約價'])
    df1=df[(df.loc[:,"履約價"] <= df_call.iloc[-1]['履約價']) & (df.loc[:,"履約價"] >=df_call.iloc[0]['履約價'])].copy().reset_index(drop=True)
    #"""
    op_df=calc_op_2_fut_df(df1)
    op_pwr=calc_op_pwr(op_df)
    call_sum=op_pwr['call小台'].sum()
    put_sum=op_pwr['put小台'].sum()
    #print(lno(),call_sum,put_sum)
    #print(lno(),op_pwr)
    fig, axes = plt.subplots(nrows=1, ncols=3)
    fig.tight_layout()
    plt.subplot(1,3,1)
    index=op_pwr.index
    plt.barh(index, op_pwr['call小台'], color ='r',label='call',alpha=0.6,height=0.6)
    plt.barh(index, op_pwr['put小台'], color ='g',label='put',alpha=0.6,height=0.6)
    plt.barh(index, op_pwr['強度'], color ='b',label='strength',alpha=0.6,height=0.2)
    plt.figtext(.1,.9,'%d'%(put_sum), fontsize=13, ha='center',color='green')
    plt.figtext(.2,.9,'%d'%(call_sum), fontsize=13, ha='center',color='red')
    for i in range(2,len(op_pwr)-2):
        if  op_pwr.loc[i]['強度']>op_pwr.loc[i-1]['強度'] and op_pwr.loc[i]['強度']>op_pwr.loc[i+1]['強度'] and \
            op_pwr.loc[i]['強度']>op_pwr.loc[i-2]['強度'] and op_pwr.loc[i]['強度']>op_pwr.loc[i+2]['強度'] :
            
            value=(op_pwr.loc[i]['履約價']*op_pwr.loc[i]['強度']+\
            op_pwr.loc[i+1]['履約價']*op_pwr.loc[i+1]['強度']+op_pwr.loc[i+2]['履約價']*op_pwr.loc[i+2]['強度']+\
            op_pwr.loc[i-1]['履約價']*op_pwr.loc[i-1]['強度']+op_pwr.loc[i-2]['履約價']*op_pwr.loc[i-2]['強度'])/\
            (op_pwr.loc[i]['強度']+op_pwr.loc[i-1]['強度']+op_pwr.loc[i-2]['強度']+op_pwr.loc[i+1]['強度']+op_pwr.loc[i+2]['強度'])
            plt.text(op_pwr.loc[i]['強度'],i,int(value),fontsize=12,ha='left',va='center',color='blue')
            #print(lno(),"high turningpoints",i, op_pwr.loc[i]['履約價'],value )
            
        #if  op_pwr.loc[i]['強度']<op_pwr.loc[i-1]['強度'] and op_pwr.loc[i]['強度']<op_pwr.loc[i+1]['強度'] and             op_pwr.loc[i]['強度']<op_pwr.loc[i-2]['強度'] and op_pwr.loc[i]['強度']<op_pwr.loc[i+2]['強度'] :
        #    print(lno(),"low turningpoints",i, op_pwr.loc[i]['履約價'] )
    #print(lno(),type(df_call.iloc[0]['到期月份(週別)']))
    #plt.title("%s OI"%(df_call.iloc[0]['到期月份(週別)']))
    plt.yticks(index,op_pwr['履約價'].values.tolist())
    #plt.xticks(np.arange(len(df_call['履約價'])),df_call['履約價'].values.tolist())
    plt.grid(True)
    plt.legend()
    
    plt.subplot(1,3,2)
    #index=np.arange(0,len(df_call['履約價']))
    index=df_call.index
    plt.barh(index, df_call['未沖銷契約數'], color ='r',label='call',alpha=0.6)
    plt.barh(index, -df_put['未沖銷契約數'], color ='g',label='put',alpha=0.6)
    #print(lno(),type(df_call.iloc[0]['到期月份(週別)']))
    #df_call_max=df_call.sort_values('未沖銷契約數', ascending=False).head(2)
    #df_put_max=df_put.sort_values('未沖銷契約數', ascending=False).head(2)
    key='未沖銷契約數'   
    dd=df_call
    for i in range(2,len(dd)-2):
        if  dd.loc[i][key]>dd.loc[i-1][key] and \
            dd.loc[i][key]>dd.loc[i+1][key] and \
            dd.loc[i][key]>dd.loc[i-2][key] and \
            dd.loc[i][key]>dd.loc[i+2][key] :
            
            value=(dd.loc[i]['履約價']*dd.loc[i][key]+\
            dd.loc[i+1]['履約價']*dd.loc[i+1][key]+dd.loc[i+2]['履約價']*dd.loc[i+2][key]+\
            dd.loc[i-1]['履約價']*dd.loc[i-1][key]+dd.loc[i-2]['履約價']*dd.loc[i-2][key])/\
            (dd.loc[i][key]+dd.loc[i-1][key]+dd.loc[i-2][key]+dd.loc[i+1][key]+dd.loc[i+2][key])
            #print(lno(),"high turningpoints",i, dd.loc[i]['履約價'],int(value) )
            plt.text(dd.loc[i][key],i,int(value),fontsize=12,ha='center',va='center')
    dd=df_put
    for i in range(2,len(dd)-2):
        if  dd.loc[i][key]>dd.loc[i-1][key] and \
            dd.loc[i][key]>dd.loc[i+1][key] and \
            dd.loc[i][key]>dd.loc[i-2][key] and \
            dd.loc[i][key]>dd.loc[i+2][key] :
            
            value=(dd.loc[i]['履約價']*dd.loc[i][key]+\
            dd.loc[i+1]['履約價']*dd.loc[i+1][key]+dd.loc[i+2]['履約價']*dd.loc[i+2][key]+\
            dd.loc[i-1]['履約價']*dd.loc[i-1][key]+dd.loc[i-2]['履約價']*dd.loc[i-2][key])/\
            (dd.loc[i][key]+dd.loc[i-1][key]+dd.loc[i-2][key]+dd.loc[i+1][key]+dd.loc[i+2][key])
            #print(lno(),"high turningpoints",i, dd.loc[i]['履約價'],int(value) )
            plt.text(-dd.loc[i][key],i,int(value),fontsize=12,ha='center',va='center')
    """
    for i in range(0,len(df_call_max)) :
        plt.text(df_call_max.iloc[i]['未沖銷契約數'],df_call_max.index.tolist()[i],df_call_max.iloc[i]['履約價'],fontsize=12,ha='left',va='center')
    for i in range(0,len(df_put_max)) :
        plt.text(-df_put_max.iloc[i]['未沖銷契約數'],df_put_max.index.tolist()[i],df_put_max.iloc[i]['履約價'],fontsize=12,ha='right',va='center')
    """
    plt.title("%s OI"%(df_call.iloc[0]['到期月份(週別)']))
    plt.yticks(index,df_call['履約價'].values.tolist())
    #plt.xticks(np.arange(len(df_call['履約價'])),df_call['履約價'].values.tolist())
    plt.grid(True)
    plt.legend()
    
    plt.subplot(1,3,3)
    plt.barh(index, df_call['diff'], color ='r',label='call',alpha=0.6)
    plt.barh(index, -df_put['diff'], color ='g',label='put',alpha=0.6)
    df_call_max=df_call.sort_values('diff', ascending=False).head(2)
    df_put_max=df_put.sort_values('diff', ascending=False).head(2)
    for i in range(0,len(df_call_max)) :
        plt.text(df_call_max.iloc[i]['diff'],df_call_max.index.tolist()[i],df_call_max.iloc[i]['履約價'],fontsize=12,ha='left',va='center')
    for i in range(0,len(df_put_max)) :
        plt.text(-df_put_max.iloc[i]['diff'],df_put_max.index.tolist()[i],df_put_max.iloc[i]['履約價'],fontsize=12,ha='right',va='center')
    plt.yticks(index,df_call['履約價'].values.tolist())
    plt.title("%s OI diff "%(df_call.iloc[0]['到期月份(週別)']))
    plt.grid(True)
    plt.legend()
    fig.set_size_inches(18, 6)
    fig.savefig(outf, dpi=100)
    plt.clf()
    plt.close(fig)

def op_plot_v1(date,debug=0):
    folder='day_report/%d%02d%02d/op'%(date.year,date.month,date.day)
    check_dst_folder(folder)
    tmp_list=[]
    list=[]
    tmp_list=get_final_OptData(date)
    if len(tmp_list)==4:
        
    
        #df = pd.DataFrame(pd.np.empty(( len(tmp_list), len(outcols))) * pd.np.nan, columns = outcols)
        for i in range(0,len(tmp_list)):
            Expiration_date =tmp_list[i].iloc[0]['到期月份(週別)']
            filen='%s/%s/oi_%s.png'%(os.getcwd(),folder,Expiration_date)
            #list.append([Expiration_date,'file://%s'%(filen)])
            list.append([Expiration_date,'op/oi_%s.png'%(Expiration_date)])
            print(lno(),filen)
            op_plot_oi(tmp_list[i],filen)
        labels = ['到期月份(週別)','OP分析圖']           
        df = pd.DataFrame.from_records(list, columns=labels) 
        df['OP分析圖'] = df['OP分析圖'].apply(lambda x: '<img src="{}" style="max-height:450px;"/>'.format(x) if x else '') 
        filen='day_report/%d%02d%02d/op.html'%(date.year,date.month,date.day)
        old_width = pd.get_option('display.max_colwidth')
        pd.set_option('display.max_colwidth', -1)
        df.to_html(filen,escape=False,index=False,sparsify=True,border=2,index_names=False)
        pd.set_option('display.max_colwidth', old_width) 
        PdfFilename='day_report/%d%02d%02d/op.pdf'%(date.year,date.month,date.day)
        options = {
            'page-size': 'B4',  
            'margin-top': '0.75in',  
            'margin-right': '0.75in',  
            'margin-bottom': '0.75in',  
            'margin-left': '0.75in',  
            'encoding': "BIG5",
            'no-outline': None
            } 
        try:    
            pdf.from_file(filen, PdfFilename,options=options)
        except:
            print(lno(),'pdf output need win')
            pass    
        #df.set_index('到期月份(週別)',drop=True)
        #print(lno(),df)
def op_down_load_job(startdate,enddate):
    now_start = startdate
    now_end = now_start + relativedelta(days=30)
    day=0
    while   now_end<=enddate :
        down_op_pc(now_start,now_end) 
        down_optData(now_start,now_end) 
        #op_plot(startdate) 
        now_start=now_end
        now_end = now_start + relativedelta(days=30)
    down_op_pc(now_start,enddate)     
    down_optData(now_start,enddate)
    now_date = enddate
    day=0
    while   now_date>=startdate :
        now_date = enddate - relativedelta(days=day)
        fut.down_data(now_date)
        down_opDelta(now_date) 
        op_plot_v1(now_date)
        gen_op_fin(startdate)                 
        day=day+1    


if __name__ == '__main__':

    
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv)==1:
        startdate=datetime.today().date()
        down_op_pc(startdate,startdate)
        down_optData(startdate,startdate)
        down_opDelta(startdate) 
        fut.down_data(startdate)
        #op_plot(startdate)
        op_plot_v1(startdate)

        
    elif sys.argv[1]=='-d' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            # 從今日往前抓一個月
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            download_twii(startdate,enddate)
            now_start = startdate
            now_end = now_start + relativedelta(days=30)
            day=0
            while   now_end<=enddate :
                down_op_pc(now_start,now_end) 
                down_optData(now_start,now_end) 
                #op_plot(startdate) 
                now_start=now_end
                now_end = now_start + relativedelta(days=30)
            down_op_pc(now_start,enddate)     
            down_optData(now_start,enddate)
            
            now_date = enddate
            day=0
            while   now_date>=startdate :
                
                now_date = enddate - relativedelta(days=day)
                fut.down_data(now_date)
                down_opDelta(now_date) 
                op_plot_v1(now_date)
                gen_op_fin(startdate)                 
                day=day+1
        else :
              print (lno(),'func -p startdata enddate') 
    elif sys.argv[1]=='-e' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            # 從今日往前抓一個月
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            now_start = startdate
            now_end = now_start + relativedelta(days=1)
            day=0
            while   now_end<=enddate :
                down_optData(now_start,now_end) 
                now_start=now_end
                now_end = now_start + relativedelta(days=1)
            down_optData(now_start,enddate)     
        else :
              print (lno(),'func -p startdata enddate')
    elif sys.argv[1]=='-d1' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            # 從今日往前抓一個月
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            now_date = enddate
            day=0
            while   now_date>=startdate :
                down_opDelta(now_date) 
                now_date = enddate - relativedelta(days=day)
                day=day+1
             
        else :
              print (lno(),'func -p startdata enddate')          
    elif sys.argv[1]=='-g' :
        if len(sys.argv)==3 :
            #參數2:開始日期 
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            #op =get_opData(startdate)
            day=0
            while  True :
                now_date = startdate + relativedelta(days=day)
                day=day+1
                #print(lno(),now_date)
                get_final_OptData(now_date)
                if day >1:
                    break
            #print(lno(),op_prev.head())
            

        else :
            print (lno(),'func -g date')          
    elif sys.argv[1]=='t' :
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        get_week_month_op_df(startdate,enddate)
    elif sys.argv[1]=='-t' :
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        down_op_pc(startdate,startdate)
        down_optData(startdate,startdate)
        down_opDelta(startdate) 
        op_plot(startdate)
        gen_op_fin(startdate)        
    elif sys.argv[1]=='-t1' :
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        op_plot_v1(startdate)
    elif sys.argv[1]=='-t2' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            # 從今日往前抓一個月
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            now_ = startdate
            day=0
            while   now_<=enddate :
                now_ = startdate + relativedelta(days=day)
                day=day+1
                op_plot(now_)
        else :
              print (lno(),'func -p startdata enddate')           
    elif len(sys.argv)==2:   
        datatime=datetime.strptime(sys.argv[1],'%Y%m%d')
        logging.info( datatime)
        get_op_url(datatime)
        
    else:
        print (lno(),"unsport ")
        sys.exit()
    
