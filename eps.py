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
import calendar
import inspect
import traceback
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib as mpl
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
def down_tse_eps(year, reason,download=1):
    dst_folder='data/eps'
    check_dst_folder(dst_folder)
    
    # 假如是西元，轉成民國
    
    url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04'
    
    #https://mops.twse.com.tw/mops/web/ajax_t163sb04
    
    filename='data/eps/tse_%s-%s.html'%(year, reason)
    out_file='data/eps/tse_%s-%s.csv'%(year, reason)
    # 偽瀏覽器
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    
    # 下載該年月的網站，並用pandas轉換成 dataframe
    print(lno(),url)
    query_params = {
        'encodeURIComponent':'1',
        'step':'1',
        'firstin':'1',
        'off':'1',
        'isQuery':'1',
        'TYPEK':'sii',
        'year':year,
        'season':reason,
    }
    if download==1:
        r = requests.post(url, data=query_params)
        if not r.ok:
            print(lno(),"Can not get data at {}".format(url))
            return 
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in r:
                file.write(chunk)
    #r.encoding = 'big5'
    #print(lno(),r.text)        
    
    df_out = pd.DataFrame()
    f = open(filename, "r",encoding='UTF-8')
    webpage_text=f.read()
    if len(webpage_text)<400 and '查詢無資料' in webpage_text:
        print(lno(),'查詢無資料')
        return
    #print(lno(),webpage_text)
    dfs = pd.read_html(StringIO(webpage_text))
    #print(lno(),dfs)
    cnt=0
    for df in dfs:
        print(lno(),df.columns)
        """
        if df.iloc[0][0]=='公司代號':
            columns=df.iloc[0].values.tolist();
            print(lno(),columns)
            df.columns=columns
            df=df.drop([0])
            df.rename(columns={'本期綜合損益總額（稅後）':'本期綜合損益總額'}, inplace=True)
            #print(lno(),df.head(1))
            columns=df.columns
        """
        if  '公司代號' in df.columns:
            columns=df.columns   
            df.rename(columns={'本期綜合損益總額（稅後）':'本期綜合損益總額'}, inplace=True)
            df['ys']=int(year)*4+int(season)-1
            if  '營業收入'in columns and '營業毛利（毛損）淨額' in columns:  
                try:
                    d=df[['公司代號','公司名稱','營業收入','營業毛利（毛損）淨額','營業利益（損失）','本期綜合損益總額','基本每股盈餘（元）','ys']].copy()     
                except:
                    print(lno(),df.iloc[0])
                    raise    
                for i in range(0,len(d)):
                    #print(lno(),df.iloc[i]['公司代號'])
                    #print(df[i:i+1])
                    comm.stock_df_to_sql_append(d.iloc[i]['公司代號'],'mix_income',d[i:i+1])
            else:
                print(lno(),df.iloc[0])        
            df1=df[['公司代號','公司名稱','基本每股盈餘（元）','本期綜合損益總額']].copy()
            df1['毛利率']=0.0
            df1['營利率']=0.0
            df1['純益率']=0.0
            df=df.replace('--',np.NaN)
            if '營業收入' in df.columns:
                if '營業毛利（毛損）淨額' in df.columns:
                    df1['毛利率']=df['營業毛利（毛損）淨額'].astype(float)/df['營業收入'].astype(float)*100
                if '營業利益（損失）' in df.columns:
                    df1['營利率']=df['營業利益（損失）'].astype(float)/df['營業收入'].astype(float)*100
                if '本期綜合損益總額' in df.columns:
                    df1['純益率']=df['本期綜合損益總額'].astype(float)/df['營業收入'].astype(float)*100    
            df1=df1.round({'毛利率': 2, '營利率': 2,'純益率': 2})    
            df_out=pd.concat([df_out,df1])
            #print(lno(),df)
            
    print(lno(),len(df_out))
    if len(df_out)>2:
        df_out.to_csv(out_file,encoding='utf-8', index=False)
    return
def down_otc_eps(year, reason,download=1):
    dst_folder='data/eps'
    check_dst_folder(dst_folder)
    
    # 假如是西元，轉成民國
    
    url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04'
    
    #https://mops.twse.com.tw/mops/web/ajax_t163sb04
    filename='data/eps/otc_%s-%s.html'%(year, reason)
    out_file='data/eps/otc_%s-%s.csv'%(year, reason)
    # 偽瀏覽器
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    
    # 下載該年月的網站，並用pandas轉換成 dataframe
    print(lno(),url)
    query_params = {
        'encodeURIComponent':'1',
        'step':'1',
        'firstin':'1',
        'off':'1',
        'isQuery':'1',
        'TYPEK':'otc',
        'year':year,
        'season':reason,
    }
    if download==1:
        r = requests.post(url, data=query_params)
        if not r.ok:
            print(lno(),"Can not get data at {}".format(url))
            return 
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in r:
                file.write(chunk)
    #r.encoding = 'big5'
    #print(lno(),r.text)        
    
    df_out = pd.DataFrame()
    f = open(filename, "r",encoding='UTF-8')
    webpage_text=f.read()
    #print (lno(),len(webpage_text))
    if len(webpage_text)<400 and '查詢無資料' in webpage_text:
        print(lno(),'查詢無資料')
        return
    dfs = pd.read_html(StringIO(webpage_text))
    cnt=0
    for df in dfs:
        """
        if df.iloc[0][0]=='公司代號':
            columns=df.iloc[0].values.tolist();
            df.columns=columns
            df=df.drop([0])
            print(lno(),columns)
        """
        if  '公司代號' in df.columns: 
            columns=df.columns   
            df['ys']=int(year)*4+int(season)-1
            if  '營業收入'in columns and '營業毛利（毛損）淨額' in columns:  
                try:
                    d=df[['公司代號','公司名稱','營業收入','營業毛利（毛損）淨額','營業利益（損失）','本期綜合損益總額','基本每股盈餘（元）','ys']].copy()     
                except:
                    print(lno(),df.iloc[0])
                    raise    
                for i in range(0,len(d)):
                    #print(lno(),df.iloc[i]['公司代號'])
                    #print(df[i:i+1])
                    comm.stock_df_to_sql_append(d.iloc[i]['公司代號'],'mix_income',d[i:i+1])
            else:
                print(lno(),df.iloc[0])
            df1=df[['公司代號','公司名稱','基本每股盈餘（元）','本期綜合損益總額']].copy()
            df1['毛利率']=0.0
            df1['營利率']=0.0
            df1['純益率']=0.0
            df=df.replace('--',np.NaN)
            if '營業收入' in columns:
                if '營業毛利（毛損）淨額' in columns:
                    df1['毛利率']=df['營業毛利（毛損）淨額'].astype(float)/df['營業收入'].astype(float)*100
                if '營業利益（損失）' in columns:
                    df1['營利率']=df['營業利益（損失）'].astype(float)/df['營業收入'].astype(float)*100
                if '本期綜合損益總額' in columns:
                    df1['純益率']=df['本期綜合損益總額'].astype(float)/df['營業收入'].astype(float)*100    
            df1=df1.round({'毛利率': 2, '營利率': 2,'純益率': 2})
            df_out=pd.concat([df_out,df1])
            #print(lno(),df)
            
    print(lno(),len(df_out))
    if len(df_out)>2:
        df_out.to_csv(out_file,encoding='utf-8', index=False)
    return    
def generate_stock_profit(flag,year,season):
    dst_folder='data/eps/final'
    check_dst_folder(dst_folder)
    out_file='data/eps/%s_%d-%d.csv'%(flag,year, season)
    if os.path.exists(out_file): 
        df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype= {'公司代號':str})
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        df_s.columns=['公司代號','公司名稱','本季eps','本期綜合損益總額','毛利率','營利率','純益率']
        if season !=1:
            prev_file='data/eps/%s_%d-%d.csv'%(flag,year, int(season)-1)
            df_prev = pd.read_csv(prev_file,encoding = 'utf-8',dtype= {'公司代號':str})
            df_prev.dropna(axis=1,how='all',inplace=True)
            df_prev.dropna(inplace=True)
            df_prev.columns=['公司代號','公司名稱','前季eps','前期綜合損益總額','前期毛利率','前期營利率','前期純益率']
            df_s=pd.merge(df_s,df_prev)
            df_s['本季損益']=df_s['本期綜合損益總額']-df_s['前期綜合損益總額']
            #print(lno(),df_s)
        else:
            df_s['本季損益']=df_s['本期綜合損益總額']
        #print(lno(),df_s)
        df_s['年度']=year
        df_s['季']=season
        df=df_s[['年度','季','公司代號','本季損益','本季eps','本期綜合損益總額','毛利率','營利率','純益率']].copy()
        
        #print(lno(),df.iloc[0:1,:])
        #print(lno(),df.dtypes)
        for i in range(0,len(df)):
            df1=df.iloc[i:i+1,:].reset_index(drop=True)
            filen='{}/{}.profit'.format(dst_folder,df1.iloc[0]['公司代號'])
            if os.path.exists(filen):
                df_s = pd.read_csv(filen,encoding = 'utf-8')
                df_s.dropna(axis=1,how='all',inplace=True)
                df_s.dropna(inplace=True)
                df_s=df_s.append(df1,ignore_index=True)
                #print(lno(),df_s)
                df_s['日期']=df_s['年度'].astype('int')*4+df_s['季'].astype('int')  
                #print(lno(),df_s['日期'])
                df_s.drop_duplicates(subset=['日期'],keep='last',inplace=True)
                df_s=df_s.sort_values(by=['日期'], ascending=False)
                df_s.drop('日期', axis=1, inplace = True)
                df_s.to_csv(filen,encoding='utf-8', index=False)
            else:
                df1.to_csv(filen,encoding='utf-8', index=False)
    else :
        print(lno(),"no file",out_file)
def gen_eps(year_str, season_str,debug=1):
    
    year=int(year_str)
    season=int(season_str)
    generate_stock_profit('tse',year,season)
    generate_stock_profit('otc',year,season)
    return 
def todate1(arrLike,year,season):  #用来计算日期间隔天数的调用的函数
    year = arrLike[year]
    season = arrLike[season]
    #print(lno(),year,season)
    #days = dataInterval(PublishedTime.strip(),ReceivedTime.strip())  #注意去掉两端空白
    fix_year=year+1911
    fix_month=season*3
    firstDayWeekDay, monthRange = calendar.monthrange(fix_year, fix_month)
    fix_day=monthRange
    
    return datetime(year=fix_year,month=fix_month,day=fix_day,)
def todate2(arrLike,year,season):  #用来计算日期间隔天数的调用的函数
    year = arrLike[year]
    season = arrLike[season]
    #print(lno(),year,season)
    #days = dataInterval(PublishedTime.strip(),ReceivedTime.strip())  #注意去掉两端空白
    
    
    return '{}({})'.format(year,season)
    
def gen_3_rate_pic(stock_no,enddate,outf):
    month=0
    cnt=0
    tmp_list=[]
    _csv='data/eps/final/%s.profit'% (stock_no)
    if os.path.exists(_csv):
        #print(lno(),_csv)
        df_s = pd.read_csv(_csv,encoding = 'utf-8',dtype= {'公司代號':str})
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        
        df_s['日期']=  df_s.apply(todate1 , axis = 1, year='年度',season='季')  
        df_s['str_date']=  df_s.apply(todate2 , axis = 1, year='年度',season='季')  
        df_s = df_s.sort_values('日期', ascending=False)
        if df_s.iloc[0]['季']!=4:
            tmp=[]
            tmp.append(0)
            df1=df_s.loc[(df_s['季'] == 4) & (df_s['日期'] <= enddate)].head(3)
            print(lno(),df1.index.values)
            tmp.extend(df1.index.values)
            df=df_s.loc[tmp]
            print(lno(),tmp)
            print(lno(),df)
            #df=pd.merge(df,df1)
        else:
            df=df_s.loc[(df_s['季'] == 4) & (df_s['日期'] <= enddate)].head(4)
        df=df.sort_values('日期', ascending=True).reset_index(drop=True)    
        df_o=df[['str_date','毛利率','營利率','純益率']]
        index=df_o.index        
        bar_width = 0.3
        A = plt.bar(index,df_o['毛利率'], bar_width, alpha=0.4,label="毛利率") 
        B = plt.bar(index+0.3,df_o['營利率'],bar_width,alpha=0.4,label="營利率") 
        C = plt.bar(index+0.6,df_o['純益率'],bar_width,alpha=0.4,label="純益率") 
        def createLabels(data):
            for item in data:
                height = item.get_height()
                plt.text(
                    item.get_x()+item.get_width()/2., 
                    height*1.05, 
                    '%.2f' % height,
                    ha = "center",
                    va = "bottom",
                )
        createLabels(A)
        createLabels(B)
        createLabels(C)
        plt.xticks(index+.3 / 2 ,df_o['str_date'])
        plt.legend() 
        plt.grid(True)
        #plt.show()
        plt.savefig(outf)
        plt.clf()
        plt.close()

def get_eps_by_stockid(stock_id,enddate,market):
    month=0
    cnt=0
    tmp_list=[]
    record_y_s=''
    while   cnt<5 :
        nowdatetime = enddate - relativedelta(months=month) 
        #print(lno(),nowdatetime)
        year=int(nowdatetime.year-1911)
        season=int((int(nowdatetime.month)+2)/3)
        _csv='data/eps/%s_%d-%d.csv'% (market,year,season )
        
        if os.path.exists(_csv):
            #print(lno(),_csv)
            tmp_list.append(year)
            tmp_list.append(season)
            df_s = pd.read_csv(_csv,encoding = 'utf-8',dtype= {'公司代號':str})
            df_s.dropna(axis=1,how='all',inplace=True)
            df_s.dropna(inplace=True)
            df=df_s.loc[df_s['公司代號'] == stock_id]
            if len(df)==1:
                tmp_list.append(df.iloc[0]['基本每股盈餘（元）'])
            else:
                tmp_list.append(np.NaN)
            _csv='data/eps/%s_%d-%d.csv'% (market,year-1,season )    
            df_s = pd.read_csv(_csv,encoding = 'utf-8',dtype= {'公司代號':str})
            df_s.dropna(axis=1,how='all',inplace=True)
            df_s.dropna(inplace=True)
            df=df_s.loc[df_s['公司代號'] == stock_id]
            if len(df)==1:
                tmp_list.append(df.iloc[0]['基本每股盈餘（元）'])
            else:
                tmp_list.append(np.NaN)
            #print(lno(),df)
            return tmp_list
            cnt=cnt+1
        month=month+3
    return []    
def get_eps_df(stock_no):
    _csv='data/eps/final/%s.profit'% (stock_no)
    if os.path.exists(_csv):
        #print(lno(),_csv)
        df_s = pd.read_csv(_csv,encoding = 'utf-8',dtype= {'公司代號':str})
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        return df_s
    return pd.DataFrame()
def get_season_high_low(df,startdate,enddate):
    df1=comm.get_df_bydate(df,startdate,enddate)
    df2=df1.copy()
    #print(lno(),df2['high'].max(),df2['low'].min(),df2['close'].mean())
    return [df2['high'].max(),df2['low'].min(),df2['close'].mean()]
def assume_eps(df_eps):
    i=0
    if len(df_eps)<=4:
        return np.nan
    if df_eps.loc[i]['季']==1:
        ##2341 ==> 1(i) + prev4(i+1)-prev1(i+4)
        act_eps=df_eps.at[i,'本季eps']+df_eps.at[i+1,'本季eps']-df_eps.at[i+4,'本季eps']
    elif df_eps.loc[i]['季']==2:
        ##3412 ==> 2(i) + prev4(i+2)-prev2(i+4)
        act_eps=df_eps.at[i,'本季eps']+df_eps.at[i+2,'本季eps']-df_eps.at[i+4,'本季eps']
    elif df_eps.loc[i]['季']==3:
        ##4123 ==> 3(i) + prev4(i+3)-prev3(i+4)
        act_eps=df_eps.at[i,'本季eps']+df_eps.at[i+3,'本季eps']-df_eps.at[i+4,'本季eps']
 
    elif df_eps.loc[i]['季']==4:
        ##1234 ==> 
        act_eps=df_eps.at[i,'本季eps']
    
    #print(lno(),df)
    return act_eps
def gen_eps_river(stock_no,enddate,years=3,debug=0):
    """
    get all season eps
    """
    df=comm.get_stock_df(stock_no)
    #print(lno(),df.tail())
    df1=get_eps_df(stock_no)
    #print(lno(),df1)
    df_eps=df1.copy()
    #print(lno(),df1)
    df_eps['fix_eps']=np.nan
    df_eps['max_pe']=np.nan
    df_eps['min_pe']=np.nan
    df_eps['avg_pe']=np.nan
    act_eps=assume_eps(df_eps)
    if debug>0:
        print(lno(),act_eps)
    #if len(df_eps)<14:
    if False:
        df_y=df_eps.loc[(df_eps['季'] == 4)].copy().reset_index(drop=True)
        df_y['fix_eps']=0
        df_y['max_pe']=np.nan
        df_y['min_pe']=np.nan
        df_y['avg_pe']=np.nan
         
        for i in range (0,len(df_y)):
            year=int(df_y.at[i,'年度'])
            season=int(df_y.at[i,'季'])
            
            if df_y.loc[i]['季']==4:
                fix_eps=df_y.loc[i]['本季eps']
                startdate=datetime.strptime('%d/01/01'%(int(year+1911)),'%Y/%m/%d')
                enddate=datetime.strptime('%d/01/01'%(int(year+1911)+1),'%Y/%m/%d')
                enddate=enddate-relativedelta(days=1)
                #print(lno(),startdate,enddate)
                list=get_season_high_low(df,startdate,enddate)
                #print(lno(),list)
                if len(list)==3:
                    df_y.at[i,'max_pe']=list[0]/fix_eps
                    df_y.at[i,'min_pe']=list[1]/fix_eps
                    df_y.at[i,'avg_pe']=list[2]/fix_eps
            #"""    
        df_y.dropna(inplace=True)

        df_y=df_y.head(3)    
        #print(lno(),df_y)
        tmp=[]
        tmp.append(act_eps)
        tmp.append(df_y['max_pe'].max())
        tmp.append(df_y['avg_pe'].max())
        tmp.append(df_y['avg_pe'].mean())
        tmp.append(df_y['avg_pe'].min())
        tmp.append(df_y['min_pe'].min())
        print(lno(),tmp)
    else:
        for i in range (2,len(df_eps)-1):
            year=int(df_eps.at[i,'年度'])
            season=int(df_eps.at[i,'季'])
            if df_eps.loc[i]['季']==1:
                ##4123 i+2(p3)i+1(p4) i(1) i-1(2) i-2(3) ==>3(i-2) + prev4(i+1) -prev3(i+2)
                try:
                    if i+2 <len(df_eps):
                        fix_eps=df_eps.at[i-2,'本季eps']+df_eps.at[i+1,'本季eps']-df_eps.at[i+2,'本季eps']
                        df_eps.at[i,'fix_eps']=fix_eps
                    
                        startdate=datetime.strptime('%d/01/01'%(int(year+1911)),'%Y/%m/%d')
                        enddate=datetime.strptime('%d/04/01'%(int(year+1911)),'%Y/%m/%d')
                        enddate=enddate-relativedelta(days=1)
                        #print(lno(),startdate,enddate)
                        list=get_season_high_low(df,startdate,enddate)
                        if len(list)==3 and fix_eps!=0:
                            df_eps.at[i,'max_pe']=list[0]/fix_eps
                            df_eps.at[i,'min_pe']=list[1]/fix_eps
                            df_eps.at[i,'avg_pe']=list[2]/fix_eps
                except:
                    print(lno(),stock_no,"eps error",i,len(df_eps))
                
                
            elif df_eps.loc[i]['季']==2:
                ##1234
                try:
                    fix_eps=df_eps.loc[i-2]['本季eps']
                    df_eps.at[i,'fix_eps']=fix_eps
                       
                    startdate=datetime.strptime('%d/04/01'%(int(year+1911)),'%Y/%m/%d')
                    enddate=datetime.strptime('%d/07/01'%(int(year+1911)),'%Y/%m/%d')
                    enddate=enddate-relativedelta(days=1)
                    #print(lno(),startdate,enddate)
                    list=get_season_high_low(df,startdate,enddate)
                    if len(list)==3 and fix_eps!=0:
                        df_eps.at[i,'max_pe']=list[0]/fix_eps
                        df_eps.at[i,'min_pe']=list[1]/fix_eps
                        df_eps.at[i,'avg_pe']=list[2]/fix_eps
                except:
                    print(lno(),stock_no,"eps error",i,len(df_eps))
            #""" 
            elif df_eps.loc[i]['季']==3:
            ##2341  i+2(1)i+1(2) i(3) i-1(4) i-2(1)==> 4- 1 +next 1
                try:
                    if i+2 <len(df_eps):
                        fix_eps=df_eps.loc[i-1]['本季eps']-df_eps.loc[i+2]['本季eps']+df_eps.loc[i-2]['本季eps']
                        df_eps.at[i,'fix_eps']=fix_eps
                        startdate=datetime.strptime('%d/07/01'%(int(year+1911)),'%Y/%m/%d')
                        enddate=datetime.strptime('%d/10/01'%(int(year+1911)),'%Y/%m/%d')
                        enddate=enddate-relativedelta(days=1)
                        #print(lno(),startdate,enddate)
                        list=get_season_high_low(df,startdate,enddate)
                        if len(list)==3 and fix_eps!=0:
                            df_eps.at[i,'max_pe']=list[0]/fix_eps
                            df_eps.at[i,'min_pe']=list[1]/fix_eps
                            df_eps.at[i,'avg_pe']=list[2]/fix_eps
                except:
                    print(lno(),stock_no,"eps error",i,len(df_eps))
            elif df_eps.loc[i]['季']==4:
            ##3412 ==> 4 -2 +n2 =>i+2(2) i+1(3) i(4) i-1(n1) i-1(n2)
                #print(lno(),df_eps)
                try:
                    if i+2 <len(df_eps):
                        fix_eps=df_eps.loc[i]['本季eps']-df_eps.loc[i+2]['本季eps'] +df_eps.loc[i-2]['本季eps']
                        df_eps.at[i,'fix_eps']=fix_eps
                        
                        startdate=datetime.strptime('%d/10/01'%(int(year+1911)),'%Y/%m/%d')
                        enddate=datetime.strptime('%d/01/01'%(int(year+1911)+1),'%Y/%m/%d')
                        enddate=enddate-relativedelta(days=1)
                        #print(lno(),startdate,enddate)
                        list=get_season_high_low(df,startdate,enddate)
                        if len(list)==3 and fix_eps!=0:
                            df_eps.at[i,'max_pe']=list[0]/fix_eps
                            df_eps.at[i,'min_pe']=list[1]/fix_eps
                            df_eps.at[i,'avg_pe']=list[2]/fix_eps
                except:
                    #print(lno(),df_eps.loc[i]['本季eps'],df_eps.loc[i+2]['本季eps'], df_eps.loc[i-2]['本季eps'])
                    print(lno(),stock_no,"eps error",i,len(df_eps))
                    
            #"""    
        #print(lno(),df_eps)
        df_eps.dropna(inplace=True)    
        #print(lno(),df_eps)
        df_eps=df_eps.head(years*4)    
        #print(lno(),df_eps)
        tmp=[]
        tmp.append(act_eps)
        tmp.append(df_eps['max_pe'].max())
        tmp.append(df_eps['avg_pe'].max())
        tmp.append(df_eps['avg_pe'].mean())
        tmp.append(df_eps['avg_pe'].min())
        tmp.append(df_eps['min_pe'].min())
        if debug>0:
            print(lno(),tmp)
    return tmp
    #print(lno(),df_eps)
def gen_eps_river_by_year(stock_no,enddate):
    """
    get all season eps
    """
    df=comm.get_stock_df(stock_no)
    #print(lno(),df.tail())
    df1=get_eps_df(stock_no)
    df_eps=df1.loc[(df1['季'] == 4)].copy().reset_index(drop=True)
    #df_eps=df1.copy()
    df_eps['fix_eps']=np.nan
    df_eps['max_pe']=np.nan
    df_eps['min_pe']=np.nan
    df_eps['avg_pe']=np.nan
     
    for i in range (0,len(df_eps)):
        year=int(df_eps.at[i,'年度'])
        season=int(df_eps.at[i,'季'])
        
        if df_eps.loc[i]['季']==4:
            fix_eps=df_eps.loc[i]['本季eps']
            startdate=datetime.strptime('%d/01/01'%(int(year+1911)),'%Y/%m/%d')
            enddate=datetime.strptime('%d/01/01'%(int(year+1911)+1),'%Y/%m/%d')
            enddate=enddate-relativedelta(days=1)
            #print(lno(),startdate,enddate)
            list=get_season_high_low(df,startdate,enddate)
            if len(list)==3:
                df_eps.at[i,'max_pe']=list[0]/fix_eps
                df_eps.at[i,'min_pe']=list[1]/fix_eps
                df_eps.at[i,'avg_pe']=list[2]/fix_eps
        #"""    
    df_eps=df_eps.head(3)    
    print(lno(),df_eps)
    tmp=[]
    tmp.append(df_eps['max_pe'].max())
    tmp.append(df_eps['min_pe'].min())
    tmp.append(df_eps['avg_pe'].max())
    tmp.append(df_eps['avg_pe'].min())
    tmp.append(df_eps['avg_pe'].mean())
    print(lno(),tmp)

def down_financial(year, reason,market,type='綜合損益彙總表',download=1):
    dst_folder='data/eps'
    check_dst_folder(dst_folder)
    
    if type == '綜合損益彙總表':
        url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04'
    elif type == '資產負債彙總表':
        url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb05'
    elif type == '營益分析彙總表':
        url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb06'
    else:
        print(lno(),'type does not match')
        return

    if year >= 1000:
        year -= 1911
    if market=='tse':
        typek='sii',
    elif market=='otc':
        typek='otc',
    else:
        print(lno(),'market does not match')
        return
    filename='data/eps/%s_%d-%d.html'%(market,year, reason)
    out_file='data/eps/%s_%d-%d.csv'%(market,year, reason)
    # 下載該年月的網站，並用pandas轉換成 dataframe
    print(lno(),url)
    query_params = {
        'encodeURIComponent':'1',
        'step':'1',
        'firstin':'1',
        'off':'1',
        'isQuery':'1',
        'TYPEK':typek,
        'year':year,
        'season':reason,
    }
    if download==1:
        r = requests.post(url, data=query_params)
        if not r.ok:
            print(lno(),"Can not get data at {}".format(url))
            return 
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in r:
                file.write(chunk)
    #r.encoding = 'big5'
    #print(lno(),r.text)        
    
    df_out = pd.DataFrame()
    f = open(filename, "r",encoding='UTF-8')
    webpage_text=f.read()
    if len(webpage_text)<400 and '查詢無資料' in webpage_text:
        print(lno(),'查詢無資料')
        return
    #print(lno(),webpage_text)
    dfs = pd.read_html(StringIO(webpage_text))
    cnt=0
    for df in dfs:
        if df.iloc[0][0]=='公司代號':
            columns=df.iloc[0].values.tolist();
            print(lno(),columns)
            df.columns=columns
            df=df.drop([0])
            df.rename(columns={'本期綜合損益總額（稅後）':'本期綜合損益總額'}, inplace=True)
            #print(lno(),df.head(1))
            columns=df.columns
            df1=df[['公司代號','公司名稱','基本每股盈餘（元）','本期綜合損益總額']].copy()
            df1['毛利率']=0.0
            df1['營利率']=0.0
            df1['純益率']=0.0
            df=df.replace('--',np.NaN)
            if '營業收入' in columns:
                if '營業毛利（毛損）淨額' in columns:
                    df1['毛利率']=df['營業毛利（毛損）淨額'].astype(float)/df['營業收入'].astype(float)*100
                if '營業利益（損失）' in columns:
                    df1['營利率']=df['營業利益（損失）'].astype(float)/df['營業收入'].astype(float)*100
                if '本期綜合損益總額' in columns:
                    df1['純益率']=df['本期綜合損益總額'].astype(float)/df['營業收入'].astype(float)*100    
            df1=df1.round({'毛利率': 2, '營利率': 2,'純益率': 2})    
            df_out=pd.concat([df_out,df1])
            #print(lno(),df)
            
    print(lno(),len(df_out))
    if len(df_out)>2:
        df_out.to_csv(out_file,encoding='utf-8', index=False)
    return
def get_last_year_eps(date):
    nowdate=date
    get_data=False
    df_s=pd.DataFrame()
    while get_data==False:
        year=str(int(nowdate.year)-1911)
        season=4
        file='data/eps/tse_%s-%s.csv'%(year, season)
        if os.path.exists(file):  
            df_s = pd.read_csv(file,encoding = 'utf-8',dtype= {'公司代號':str})
            file='data/eps/otc_%s-%s.csv'%(year, season)
            if os.path.exists(file):  
                df_otc = pd.read_csv(file,encoding = 'utf-8',dtype= {'公司代號':str})
                df_s=pd.concat([df_s,df_otc]).reset_index(drop=True)
            df_s['year']=int(nowdate.year)
            get_data=True
            #print(lno(),df_s)
        else:    
            nowdate = nowdate - relativedelta(years=1)
    return df_s     
def get_last_year_income(date):
    nowdate=date
    get_data=False
    df_s=pd.DataFrame()
    while get_data==False:
        year=str(int(nowdate.year)-1911)
        season=4
        file='data/eps/tse_%s-%s'%(year, season)
        if os.path.exists(file):  
            df_s = pd.read_html(file,encoding = 'utf8')
            print(lno(),df_s)
            """
            file='data/eps/otc_%s-%s.csv'%(year, season)
            if os.path.exists(file):  
                df_otc = pd.read_csv(file,encoding = 'utf-8',dtype= {'公司代號':str})
                df_s=pd.concat([df_s,df_otc]).reset_index(drop=True)
            df_s['year']=int(nowdate.year)
            """
            get_data=True
            print(lno(),df_s)
        else:    
            nowdate = nowdate - relativedelta(years=1)
    return df_s   
 
"""
filename='data/eps/tse_%s-%s'%(year, reason)
    
    f = open(filename, "r",encoding='UTF-8')
    webpage_text=f.read()
    if len(webpage_text)<400 and '查詢無資料' in webpage_text:
        print(lno(),'查詢無資料')
        return
    #print(lno(),webpage_text)
    dfs = pd.read_html(StringIO(webpage_text))
    cnt=0
    for df in dfs:
        if df.iloc[0][0]=='公司代號':
            columns=df.iloc[0].values.tolist();
            print(lno(),columns)
            df.columns=columns
產生落於低位階的股票
"""
def gen_buy_mode1(date):
    df =get_last_year_eps(date) 
    df=df[df['基本每股盈餘（元）']>0].reset_index(drop=True)
    for i in range(0,len(df)):
        df_eps=get_eps_df(df.iloc[i]['公司代號'])
        
        print(lno(),df.iloc[i]['公司代號'])
        print(lno(),df_eps)
    #print(lno(),df)
if __name__ == '__main__':

    sns.set()
    mpl.rcParams['font.family'] = 'sans-serif'
    mpl.rcParams['font.sans-serif'] = 'SimHei'
    if len(sys.argv)==1:
        startdate=datetime.today().date()
        down_tse_monthly_report(int(startdate.year),int(startdate.month)-1)
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
                year=str(int(now_date.year)-1911)
                season=str(int((int(now_date.month)+2)/3))
                print(lno(),year,season)
                down_tse_eps(year,season,0)
                down_otc_eps(year,season,0)

                now_date = now_date + relativedelta(months=3)
   
        else :
              print (lno(),'func -p startdata enddate') 
         
    elif sys.argv[1]=='-g' :
        """
        read down eps file to csv/eps/final/stock_id.profit
        """
        if len(sys.argv)==4 :
            """
            first 20130101
            end:now
            """
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            now_date = startdate 
            while   now_date<=enddate :
                year=int(now_date.year)-1911
                season=int((int(now_date.month)+2)/3)
                print(lno(),year,season)
                gen_eps(year,season)
                now_date = now_date + relativedelta(months=3)

        else :
            print (lno(),'func -g date')          
    elif sys.argv[1]=='-t' :
        """
        test get stock 3 rate
        """
        if len(sys.argv)==4 :
            """
            stock_no=sys.argv[2]  
            date=sys.argv[3]
            """
            stock_no=sys.argv[2]
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            
            gen_3_rate_pic(stock_no,enddate,'test1.png')

        else :
            print (lno(),'func -t stock_no date')  
    elif sys.argv[1]=='t1' :

        if len(sys.argv)==4 :
            """
            stock_no=sys.argv[2]  
            date=sys.argv[3]
            """
            stock_no=sys.argv[2]
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            gen_eps_river(stock_no,enddate)
            #gen_eps_river_by_year(stock_no,enddate)

        else :
            print (lno(),'func -t stock_no date')  
    elif sys.argv[1]=='g1' :
        #try:
        enddate=datetime.strptime(sys.argv[2],'%Y%m%d')
        gen_buy_mode1(enddate)

        #except:
        #    print (lno(),'func g1 date')          
    elif len(sys.argv)==3:
        ## input parameter : 108 1   
        year=int(sys.argv[1])  
        season=int(sys.argv[2])
       
        #down_financial(year,reason,'tse',type='綜合損益彙總表',download=1)
        down_tse_eps(year,season,1)
        down_otc_eps(year,season,1)
        #gen_eps(year,season)
        
    else:
        print (lno(),"usage: eps.py 108 1 ")
        sys.exit()
    
    