# -*- coding: utf-8 -*-

import csv
import os
import sys
import time
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import requests
from io import StringIO
from inspect import currentframe, getframeinfo

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
def down_tse_monthly_report(year, month):
    dst_folder='data/revenue'
    check_dst_folder(dst_folder)
    
    # 假如是西元，轉成民國
    if year > 1990:
        year -= 1911
    url = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_'+str(year)+'_'+str(month)+'_0.html'
    if year <= 98:
        url = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_'+str(year)+'_'+str(month)+'.html'
    #https://mops.twse.com.tw/nas/t21/otc/t21sc03_108_6_0.html
    #https://mops.twse.com.tw/nas/t21/sii/t21sc03_108_6_0.html
    #https://mops.twse.com.tw/nas/t21/sii/t21sc03_108_7_1.html
    filename='data/revenue/tse_%d-%02d'%(year, month)
    out_file='data/revenue/tse_%d-%02d.csv'%(year, month)
    # 偽瀏覽器
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    
    # 下載該年月的網站，並用pandas轉換成 dataframe
    print(lno(),url)

    r = requests.get(url, headers=headers)
    if not r.ok:
        print(lno(),"Can not get data at {}".format(url))
        return 
    with open(filename, 'wb') as file:
        # A chunk of 128 bytes
        for chunk in r:
            file.write(chunk)
            
    #r.encoding = 'big5'
    #print(lno(),r.text)
    f = open(filename, "r")
    dfs = pd.read_html(StringIO(f.read()), encoding='big5')
    #for df in dfs:
    #    print(lno(),df)
    #print(lno(),dfs)
    df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5])
    print(lno(),df.columns)
    if 'levels' in dir(df.columns):
        df.columns = df.columns.get_level_values(1)
    else:
        df = df[list(range(0,10))]
        #print(lno(),df.tail())
        column_index = df.index[(df[0] == '公司代號')][0]
        df.columns = df.iloc[column_index]
    print(lno(),len(df))
    df['當月營收'] = pd.to_numeric(df['當月營收'], 'coerce')
    df = df[~df['當月營收'].isnull()]
    df = df[df['公司代號'] != '合計']
    
    # 偽停頓
    #time.sleep(5)
    df.to_csv(out_file,encoding='utf-8', index=False)
    print(lno(),len(df))
    if year <= 98:
        return 
    url = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_'+str(year)+'_'+str(month)+'_1.html'

    filename='data/revenue/tse_%d-%02d_1'%(year, month)
    out_file='data/revenue/tse_%d-%02d_1.csv'%(year, month)

    # 下載該年月的網站，並用pandas轉換成 dataframe
    print(lno(),url)

    r = requests.get(url, headers=headers)
    if not r.ok:
        print(lno(),"Can not get data at {}".format(url))
        return 
    with open(filename, 'wb') as file:
        # A chunk of 128 bytes
        for chunk in r:
            file.write(chunk)
            
    #r.encoding = 'big5'
    #print(lno(),r.text)
    f = open(filename, "r")
    dfs = pd.read_html(StringIO(f.read()), encoding='big5')
    df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5])
    print(lno(),df.columns)
    if 'levels' in dir(df.columns):
        df.columns = df.columns.get_level_values(1)
    else:
        df = df[list(range(0,10))]
        #print(lno(),df.tail())
        column_index = df.index[(df[0] == '公司代號')][0]
        df.columns = df.iloc[column_index]
    print(lno(),len(df))
    df['當月營收'] = pd.to_numeric(df['當月營收'], 'coerce')
    df = df[~df['當月營收'].isnull()]
    df = df[df['公司代號'] != '合計']
    
    # 偽停頓
    #time.sleep(5)
    df.to_csv(out_file,encoding='utf-8', index=False)
    print(lno(),len(df))
    
    return df

def down_otc_monthly_report(year, month):
    dst_folder='data/revenue'
    check_dst_folder(dst_folder)
    
    # 假如是西元，轉成民國
    if year > 1990:
        year -= 1911
    url = 'https://mops.twse.com.tw/nas/t21/otc/t21sc03_'+str(year)+'_'+str(month)+'_0.html'
    #if year <= 98:
    #    url = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_'+str(year)+'_'+str(month)+'.html'
    #https://mops.twse.com.tw/nas/t21/otc/t21sc03_108_6_0.html
    #https://mops.twse.com.tw/nas/t21/sii/t21sc03_108_6_0.html
    filename='data/revenue/otc_%d-%02d'%(year, month)
    out_file='data/revenue/otc_%d-%02d.csv'%(year, month)
    # 偽瀏覽器
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    
    # 下載該年月的網站，並用pandas轉換成 dataframe
    print(lno(),url)
    r = requests.get(url, headers=headers)
    if not r.ok:
        print(lno(),"Can not get data at {}".format(url))
        return 
    with open(filename, 'wb') as file:
        # A chunk of 128 bytes
        for chunk in r:
            file.write(chunk)
            
    #r.encoding = 'big5'
    #print(lno(),r.text)
    
    #f = open(filename, 'r', encoding='big5',errors='ignore')
    f = open(filename, 'r', encoding='big5hkscs')
    webpage_text=f.read()
    #for i in webpage_text:
    #    print (i)
    #dfs = pd.read_html(StringIO(f.read()), encoding='big5')
    dfs = pd.read_html(StringIO(webpage_text))
    #for df in dfs:
    #    print(lno(),df)
    #print(lno(),dfs)
    df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5])
    print(lno(),df.columns)
    if 'levels' in dir(df.columns):
        df.columns = df.columns.get_level_values(1)
    else:
        df = df[list(range(0,10))]
        #print(lno(),df.tail())
        column_index = df.index[(df[0] == '公司代號')][0]
        df.columns = df.iloc[column_index]
    print(lno(),len(df))
    df['當月營收'] = pd.to_numeric(df['當月營收'], 'coerce')
    df = df[~df['當月營收'].isnull()]
    df = df[df['公司代號'] != '合計']
    
    # 偽停頓
    #time.sleep(5)
    df.to_csv(out_file,encoding='utf-8', index=False)
    print(lno(),len(df))
    if year <= 98:
        return
    url = 'https://mops.twse.com.tw/nas/t21/otc/t21sc03_'+str(year)+'_'+str(month)+'_1.html'

    filename='data/revenue/otc_%d-%02d_1'%(year, month)
    out_file='data/revenue/otc_%d-%02d_1.csv'%(year, month)
    
    # 下載該年月的網站，並用pandas轉換成 dataframe
    print(lno(),url)
    r = requests.get(url, headers=headers)
    if not r.ok:
        print(lno(),"Can not get data at {}".format(url))
        return 
    with open(filename, 'wb') as file:
        # A chunk of 128 bytes
        for chunk in r:
            file.write(chunk)
            
    f = open(filename, 'r', encoding='big5hkscs')
    webpage_text=f.read()
    #for i in webpage_text:
    #    print (i)
    #dfs = pd.read_html(StringIO(f.read()), encoding='big5')
    try:
        dfs = pd.read_html(StringIO(webpage_text))
    except:
        return
 
    df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5])
    print(lno(),df.columns)
    if 'levels' in dir(df.columns):
        df.columns = df.columns.get_level_values(1)
    else:
        df = df[list(range(0,10))]
        #print(lno(),df.tail())
        column_index = df.index[(df[0] == '公司代號')][0]
        df.columns = df.iloc[column_index]
    print(lno(),len(df))
    df['當月營收'] = pd.to_numeric(df['當月營收'], 'coerce')
    df = df[~df['當月營收'].isnull()]
    df = df[df['公司代號'] != '合計']
    
    # 偽停頓
    #time.sleep(5)
    df.to_csv(out_file,encoding='utf-8', index=False)
    print(lno(),len(df))
    
    return 
def gen_revenue_good_list(enddate):
    #market='final'
    revenue_csv='data/revenue/final/%d-%02d.csv'% (enddate.year-1911,enddate.month )
    print(lno(),revenue_csv)
    if os.path.exists(revenue_csv):
        df_s = pd.read_csv(revenue_csv,encoding = 'utf-8',dtype= {'公司代號':str})
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print(lno(),df_s)
        #df=df_s[(df_s.loc[:,"去年同月增減(%)"] >= 20) | (df_s.loc[:,"前期比較增減(%)"] >= 15)]
        df=df_s[(df_s.loc[:,"去年同月增減(%)"] >= 20)].copy().reset_index(drop=True)
        prev_month = enddate - relativedelta(months=1)
        prevf='data/revenue/final/%d-%02d.csv'% (prev_month.year-1911,prev_month.month )
        if  os.path.exists(prevf):
            df_p = pd.read_csv(prevf,encoding = 'utf-8',dtype= {'公司代號':str})
            df_p.dropna(axis=1,how='all',inplace=True)
            df_p.dropna(inplace=True)
            p_b20_list=df_p[(df_p.loc[:,"去年同月增減(%)"] >= 20)]['公司代號'].tolist()
            #print(lno(),p_b20_list)
            
            
        print(lno(),len(df))
        df['pmonth']=True
        for i in range (0,len(df)):
            stock_no=df.iloc[i]['公司代號']
            if stock_no in p_b20_list:
                df.at[i,'pmonth']=False
        df1=df[(df.loc[:,"pmonth"] == True)].copy().reset_index(drop=True)
        print(lno(),len(df1))
        df1=df1[['公司代號','公司名稱','去年同月增減(%)','前期比較增減(%)']]
        #print(df1)
        out_file='csv/rev_good.csv'
        df1.to_csv(out_file,encoding='utf-8', index=False)
        
def gen_revenue_final_file(enddate):
    _list=[]
    _list.append('data/revenue/tse_%d-%02d.csv'% (enddate.year-1911,enddate.month ))
    _list.append('data/revenue/tse_%d-%02d_1.csv'% (enddate.year-1911,enddate.month ))
    _list.append('data/revenue/otc_%d-%02d.csv'% (enddate.year-1911,enddate.month ))
    _list.append('data/revenue/otc_%d-%02d_1.csv'% (enddate.year-1911,enddate.month ))
    check_dst_folder('data/revenue/final')
    df_list=[]
    for revenue_csv  in _list:
        #print(lno(),revenue_csv)
        if os.path.exists(revenue_csv):
            df = pd.read_csv(revenue_csv,encoding = 'utf-8',dtype= {'公司代號':str})
            df_list.append(df)
            #df1=dfs[['公司代號','公司名稱','去年同月增減(%)','前期比較增減(%)']]
    dfs=pd.concat(df_list)
    dfs.dropna(axis=1,how='all',inplace=True)
    dfs.dropna(inplace=True)
    out_file='data/revenue/final/%d-%02d.csv'% (enddate.year-1911,enddate.month )
    dfs.to_csv(out_file,encoding='utf-8', index=False)
    print(lno(),dfs)
def get_revenue_by_stockid_bydate(stock_id,date):
    revenue_csv='data/revenue/final/%d-%02d.csv'% (date.year-1911,date.month )
    if os.path.exists(revenue_csv):
        df_s = pd.read_csv(revenue_csv,encoding = 'utf-8',dtype= {'公司代號':str})
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        df=df_s.loc[df_s['公司代號'] == stock_id]
        if len(df)==1:
            print(lno(),df)
            return df
        

    return pd.DataFrame()            
def get_revenue_by_stockid(stock_id,enddate):
    month=0
    print(lno(),type(stock_id),stock_id,enddate)
    while   month<=3 :
        nowdatetime = enddate - relativedelta(months=month) 
        revenue_csv='data/revenue/final/%d-%02d.csv'% (nowdatetime.year-1911,nowdatetime.month )
        if os.path.exists(revenue_csv):
            df_s = pd.read_csv(revenue_csv,encoding = 'utf-8',dtype= {'公司代號':str})
            df_s.dropna(axis=1,how='all',inplace=True)
            df_s.dropna(inplace=True)
            df=df_s.loc[df_s['公司代號'] == stock_id]
            
            if len(df)==1:
                print(lno(),df)
                return nowdatetime.month,df
        
        month=month+1       
    return nowdatetime.month,pd.DataFrame()    
if __name__ == '__main__':

    
    if len(sys.argv)==1:
        startdate=datetime.today().date()
        #down_tse_monthly_report(int(startdate.year),int(startdate.month)-1)
        down_otc_monthly_report(int(startdate.year),int(startdate.month)-1)
        #down_op_pc(startdate,startdate)
        #down_optData(startdate,startdate)
    elif sys.argv[1]=='-d' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            # 從今日往前抓一個月
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            now_date = startdate 
            while   now_date<=enddate :
                down_tse_monthly_report(int(now_date.year),int(now_date.month))
                down_otc_monthly_report(int(now_date.year),int(now_date.month))
                gen_revenue_final_file(now_date)
                now_date = now_date + relativedelta(months=1)
   
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
    elif sys.argv[1]=='good' :
        if len(sys.argv)==3 :
            #參數2:開始日期 
            datatime=datetime.strptime(sys.argv[2],'%Y%m%d')
            gen_revenue_good_list(datatime)
        else :
            print (lno(),'func -g date')   
            
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
            get_revenue_by_stockid(stock_id,datatime)
        else :
            print (lno(),'func -g date')        
    elif len(sys.argv)==2:   
        datatime=datetime.strptime(sys.argv[1],'%Y%m%d')
        down_tse_monthly_report(int(datatime.year),int(datatime.month))
        down_otc_monthly_report(int(datatime.year),int(datatime.month))
        gen_revenue_final_file(datatime)
    else:
        print (lno(),"unsport ")
        sys.exit()
    
    