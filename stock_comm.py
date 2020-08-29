# -*- coding: utf-8 -*-
#import grs
import csv
import os
import sys
import time
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import inspect
import codecs 
from inspect import currentframe, getframeinfo
import inspect
import traceback
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import scipy.signal as signal 
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR, Float, Integer,DateTime,Date
import platform
import stock_big3
DEBUG=1
debug=0
LOG=1
"""
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def ppp(string):
    if DEBUG:
        frame = inspect.currentframe()
        stack_trace = traceback.format_stack(frame)
        logging.info(stack_trace[:-1])
    if LOG:
        logging.info(string)
"""
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)

 
    
def get_date():
    if len(sys.argv)==1:
        today= datetime.today().date()
        dataday=datetime(year=today.year,month=today.month,day=today.day,)
        print (dataday)
    else:   
        dataday=datetime.strptime(sys.argv[1],'%Y%m%d')
    return dataday
#"""    


def calc_bpwr(row):
    diff=float(row['diff'])
    #print lno(),type(df.iloc[i]['close'])
    try:
        close=float(row['close'])
    except:
        print (lno(),row['close'])
    try:    
        open=float(row['open'])
    except:
        print (lno(),row['open'])
    high=float(row['high'])
    low=float(row['low'])
    vol=float(row['vol'])  
    prev_close=close-diff  
    if diff>0:
        buy1=abs(open-prev_close)
        sell1=abs(open-low)
        buy2=abs(high-low)
        sell2=abs(high-close)
    else:
        buy1=abs(high-open)
        sell1=abs(prev_close-open)
        buy2=abs(close-low)
        sell2=abs(high-low) 
    total_buy=buy1+buy2
    total_sell=sell1+sell2
    if (total_buy+total_sell) ==0:
        return 0
    buy_pwr=vol*total_buy/(total_buy+total_sell)
    sell_pwr=vol*total_buy/(total_buy+total_sell)
    return buy_pwr
def calc_spwr(row):
    return row['vol']-row['b_pwr']
def calc_ma5_20(row):
    per=(row['MA_5']-row['MA_20'])/row['MA_20']

    return per


def twdate2datetime64(x):
    #print(lno(),x,type(x))
    
    if '-' in x:
        tmp_list=x.split ('-')    
    elif '/' in  x:    
        tmp_list=x.split ('/')
    else:
        print(lno(),x,"wrong date format")
        return 0
    #print(lno(),tmp_list)    
    try :
        if int(tmp_list[0])>=1911:
            tmp_str="%d%0s%0s"%(int(tmp_list[0]),tmp_list[1],tmp_list[2])
        else :    
            tmp_str="%d%0s%0s"%(int(tmp_list[0])+1911,tmp_list[1],tmp_list[2])
        #print(lno(),tmp_str)    
        fin=datetime.strptime(tmp_str,'%Y%m%d')
    except :
    #print tmp_list[0]
        print (lno(),tmp_list[0])
        pass
    return np.datetime64(fin)
def get_df_bydate(df1,startdate,enddate):
    #print lno(),df1['date'].dtype,type(enddate) 
    df=df1[(df1.loc[:,"date"] <= np.datetime64(enddate)) & (df1.loc[:,"date"] >= np.datetime64(startdate))]
    return df         
def get_df_bydate_nums(df1,nums,enddate):
    #print lno(),df1['date'].dtype,type(enddate) 
    try:
        df=df1[(df1.loc[:,"date"] <= np.datetime64(enddate)) ]
        df=df.tail(nums)
        return df
    except:
        print(lno(),df1)
        raise
     
def get_stock_df(stock_no):
    dstpath='%s/stock_data/%s.csv'%(datafolder(),stock_no)
    #print lno(),dstpath
    if not os.path.exists(dstpath): 
        return pd.DataFrame()
 
    dtypes= {'vol':np.int64, 'cash': np.int64,'open':np.float64, 'high': np.float64,'low':np.float64, 'close': np.float64,'diff':np.float64}  
    dateparse = lambda dates: pd.datetime.strptime(dates,'%Y-%m-%d')
    df = pd.read_csv(dstpath,encoding = 'utf-8',parse_dates=['date'], date_parser=dateparse,dtype=dtypes)
    #df.drop('cash', axis=1, inplace = True)
    #df.drop('Tnumber', axis=1, inplace = True)
    df=df.fillna(method='ffill')
    """
    df.dropna(axis=1,how='all',inplace=True)
    df.dropna(inplace=True)
    df.columns = ['date', 'vol', 'cash', 'open', 'high','low','close','diff','Tnumber','stock_name']
    df=df.replace('--',np.NaN)
    df=df.replace('---',np.NaN)
    df=df.replace('----',np.NaN)
    df=df.fillna(method='ffill')
    df=df.dropna(how='any',axis=0)
    
    df['date']=[twdate2datetime64(x) for x in df['date'] ]
    df['open']=df['open'].astype('float64')
    df['high']=df['high'].astype('float64')
    df['low']=df['low'].astype('float64')
    df['close']=df['close'].astype('float64')
    """
    return df  
def get_stock_df_old(stock_no):
    dstpath='data/%s.csv'%(stock_no)
    #print lno(),dstpath
    if not os.path.exists(dstpath): 
        return pd.DataFrame()
    df = pd.read_csv(dstpath,encoding = 'utf-8')
    df.dropna(axis=1,how='all',inplace=True)
    df.dropna(inplace=True)
    df.columns = ['date', 'vol', 'cash', 'open', 'high','low','close','diff','Tnumber','stock_name']
    df.drop('cash', axis=1, inplace = True)
    df.drop('Tnumber', axis=1, inplace = True)
    df=df.replace('--',np.NaN)
    df=df.replace('---',np.NaN)
    df=df.replace('----',np.NaN)
    df=df.fillna(method='ffill')
    df=df.dropna(how='any',axis=0)
    #print(lno(),df.dtypes)
    #df['date']=[twdate2datetime64(x) for x in df['date'] ]
    def str2time64(x):
        return np.datetime64(datetime.strptime(x,'%Y-%m-%d'))
    df['date']=df['date'].apply(str2time64)
    df['open']=df['open'].astype('float64')
    df['high']=df['high'].astype('float64')
    df['low']=df['low'].astype('float64')
    df['close']=df['close'].astype('float64')
    return df         
def get_stock_df_sql(stock_no):
    stk=stock_data()
    return stk.get_df(stock_no)
    
def get_prev_month_date(date,mons):
    return date- relativedelta(months=mons)
def get_stock_df_by_startdate(stock_no,date):
    df1=get_stock_df(stock_no)
    df=df1[(df1.loc[:,"date"] >= np.datetime64(date)) ].copy().reset_index(drop=True)
    return df


def get_stock_kline_df(stock_no,enddate,month=3):
    startdate=enddate- relativedelta(months=month)
    df_in = pd.DataFrame()
    df1 = pd.DataFrame()
    df0=get_stock_df(stock_no)
    #print lno(),startdate,enddate
    stock_df=get_df_bydate(df0,startdate,enddate)
    #print lno(),stock_df
    #dist_df=get_tdcc_dist_df(stock_no,startdate,enddate)
    #fin_df=pd.merge(stock_df,dist_df,how='inner')
    
    df=stock_df.sort_values(by='date', ascending=True)
    df=df.replace('--',np.NaN)
    df=df.replace('---',np.NaN)
    df=df[df.close!=np.NaN]
    df.reset_index(inplace=True)
    #print lno(),df['b_pwr']
    try:
        df['b_pwr'] = df.apply(calc_bpwr, axis=1)
    except:
        print (lno(),df)
    df['s_pwr'] = df.apply(calc_spwr, axis=1)
    #print lno(),fin_df.tail(20)
    ma_list = [5, 20]
    for ma in ma_list:
        df['MA_' + str(ma)] = df['close'].rolling(window=ma,center=False).mean()
    #print lno(),df[['date','close','MA_5']]       
    df['MA5_MA20']=df['MA_5']-df['MA_20']  
    df['bs'] = df['s_pwr'].rolling(window=20,center=False).mean()/df['b_pwr'].rolling(window=20,center=False).mean()        
    return df        
def getKey(item):
    #print item[0]
    return item[0]

def get_tdcc_dist_df(stock_no,startdate,enddate):
    tdcc_date_path='csv/tdcc_date.csv'
    date_df = pd.read_csv(tdcc_date_path,encoding = 'big5',header=None)
    date_df.dropna(axis=1,how='all',inplace=True)
    date_df.columns = ['date_str']
    #print (lno(),startdate,enddate)
    
    date_df['date'] =  pd.to_datetime(date_df['date_str'], format='%Y%m%d')
    sample_df=get_df_bydate(date_df,startdate,enddate)
    #print (lno(),sample_df.head(4))
    tdcc_dist_file=('data/dist/dist_%(stock)s.csv')% {'stock': stock_no }
    #print (lno(),tdcc_dist_file)
    outcols = ['date','t_stocks','t_persons','avg','>400_stocks','>400_percents','>400_persons','p400-600','p600-800','p800-1000','>1000_persons','>1000_percents','price','du1','du2','du3']       
    try :
        df = pd.read_csv(tdcc_dist_file,encoding = 'utf8',header=None)
    except:
        print  (lno(),'error',tdcc_dist_file)
        df = pd.DataFrame(pd.np.empty(( 1, len(outcols))) * pd.np.nan, columns = outcols)
    df.columns = outcols
    df.drop('du1', axis=1, inplace = True)
    df.drop('du2', axis=1, inplace = True)
    df.drop('du3', axis=1, inplace = True)
    df.drop('price', axis=1, inplace = True)
    df.drop('p400-600', axis=1, inplace = True)
    df.drop('p600-800', axis=1, inplace = True)
    df.drop('p800-1000', axis=1, inplace = True)
    df.drop('avg', axis=1, inplace = True)
    df['date'] =  pd.to_datetime(df['date'], format='%Y%m%d')
    #dist_df.dropna(axis=1,how='all',inplace=True)    
    #print (lno(),df)
    for i in range(0, len(sample_df)):
        #print (lno(),sample_df.iloc[i]['date_str'])
        if len(df[df.loc[:,"date"] == sample_df.iloc[i]['date']]) == 0 :
            
            tdcc_dist_file=('data/csv/dist/%(stock)s/%(ymd)s_dist.csv')% {'stock': stock_no,'ymd':sample_df.iloc[i]['date_str'] }
            #print (lno(),tdcc_dist_file)
            try :
                dist_df = pd.read_csv(tdcc_dist_file,encoding = 'utf8')
            except:
                print  (lno(),'error',tdcc_dist_file)   
                continue
            dist_list=[]
            dist_list.append(sample_df.iloc[i]['date'])
            t_stocks= dist_df['a3'].sum()   
            t_persions= dist_df['a2'].sum()
            dist_list.append(round(t_stocks/1000))
            dist_list.append(t_persions)
            persion=0
            stocks=0
            percent=0.0
            # >=400 middle
            for j in range (11,15):
                #print lno(),dist_df.iloc[j]['a2'],dist_df.iloc[j]['a1']
                persion+=dist_df.iloc[j]['a2']
                stocks+=dist_df.iloc[j]['a3']
                percent+=dist_df.iloc[j]['a4']  
            dist_list.append(round(stocks/1000))
            dist_list.append(percent)
            dist_list.append(persion)
            dist_list.append(dist_df.iloc[14]['a2'])
            dist_list.append(dist_df.iloc[14]['a4'])
           # print (lno(),df.tail(5))
            df.loc[-1] = dist_list
            df.index = df.index + 1
                 
            #print (lno(),df.tail(5))
    df.sort_values(by='date', ascending=True,inplace = True)
    df=df.reset_index(drop=True)
    #print (lno(),df)
    return df
                        
def get_trend_ex(datatime):
    fpath='csv/trend.csv'
    twse_list=[]
    rec_date=[]
    rec=[[0,0,0],[0,0,0],[0,0,0],[0,0,0]]
    #print lno(),"test"
    try:
        with open(fpath) as csv_file:
            csv_data = csv.reader(csv_file)
            for i in csv_data:
                tmp_list=[]
                tmp_list.append(datetime.strptime(i[0],'%Y-%m-%d').date())
                tmp_list.append(int(i[1]))
                tmp_list.append(int(i[2]))
                tmp_list.append(int(i[3]))
                tmp_list.append(int(i[4]))
                tmp_list.append(int(i[5]))
                tmp_list.append(int(i[6]))
                tmp_list.append(int(i[7]))
                tmp_list.append(int(i[8]))
                tmp_list.append(int(i[9]))
                tmp_list.append(int(i[10]))
                tmp_list.append(int(i[11]))
                tmp_list.append(int(i[12]))
                if tmp_list[0]==datatime.date() :
                    return tmp_list
                twse_list.append( tmp_list)
                rec_date.append(datetime.strptime(i[0],'%Y-%m-%d').date())
    except IndexError:
        print (lno(),'IndexError')
        pass
    except IOError:
        print (lno(),'IOError')
        pass
    fin=[]      
    for j in range(1):
        nodata=0
        rec=[[0,0],[0,0],[0,0],[0,0],[0,0],[0,0]]
        nodata=0
        nowdatetime = datatime - relativedelta(days=j)
        #print nowdatetime
        tmp_list=[]
        if  nowdatetime.date() in rec_date:
            print  ( nowdatetime.date(),'in list')
            print (lno(),'test')
            cnt+=1
        else:   
            if debug==1:
                print (lno(),nowdatetime)
            nodata=0
            datanum=0
            tot=0
            #for i in TWSENo().all_stock_no+OTCNo().all_stock_no:
            twse_NO_list=get_TWSE_NO()
            otc_NO_list=get_OTC_NO()
            times1 = datetime.now()
            for i in twse_NO_list+otc_NO_list:
                tot+=1
                #df=get_stock_df(i)
                #df1=get_df_bydate_nums(df,40,nowdatetime)
                #print (lno(),i)
                df1=get_stock_df_bydate_nums(i,40,nowdatetime)
                
                if len(df1[(df1.loc[:,"date"] == np.datetime64(nowdatetime)) ])==0 :
                    print(lno(),i)
                    print(lno(),df1.tail(2))
                    nodata+=1
                    continue
                datanum+=1    
                ma_list = [5,10,20]
                #print(lno(),df1.tail(1))
                for ma in ma_list:
                    df1.loc[:,'MA_' + str(ma)] = df1['close'].rolling(window=ma,center=False).mean()
                ma5=df1.iloc[-1]['MA_5']
                ma10=df1.iloc[-1]['MA_10']  
                ma20=df1.iloc[-1]['MA_20']
                pma5=df1.iloc[-2]['MA_5']   
                pma10=df1.iloc[-2]['MA_10'] 
                pma20=df1.iloc[-2]['MA_20'] 
                #stock = Stock(i,nowdatetime)
                if nodata>40 and datanum==0 :
                    print (lno(),'ddd11',tot)
                    break
                if ma5>ma10 and ma10>ma20 :
                    if i in twse_NO_list :
                        rec[0][0]+=1
                    else: 
                        rec[3][0]+=1    
                elif ma5<ma10 and ma10<ma20 :
                    if i in twse_NO_list :
                        rec[0][1]+=1
                    else: 
                        rec[3][1]+=1
                diff=ma5-ma20
                pdiff=pma5-pma20
                if diff >0 and diff >pdiff :  ##多頭
                    if i in twse_NO_list :
                        rec[1][0]+=1  
                    else: 
                        rec[4][0]+=1
                elif diff >0 :    ##多頭背離 
                    if i in twse_NO_list :
                        rec[1][1]+=1
                    else: 
                        rec[4][1]+=1
                elif  diff <0 and diff <pdiff :
                    if i in twse_NO_list :
                        rec[2][0]+=1
                    else: 
                        rec[5][0]+=1
                elif diff <0 :
                    if i in twse_NO_list :
                        rec[2][1]+=1
                    else: 
                        rec[5][1]+=1
                                                                    
            #break
            #print nodata  
            times2 = datetime.now()   
            print('Time spent: '+ str(times2-times1))    
            """
            if nodata>20:
                print (lno(),'>20')
                continue
            """    
            tmp_list.append(nowdatetime.date())    
            tmp_list.append(rec[0][0])
            tmp_list.append(rec[0][1])
            tmp_list.append(rec[1][0])
            tmp_list.append(rec[1][1])
            tmp_list.append(rec[2][0])
            tmp_list.append(rec[2][1])
            tmp_list.append(rec[3][0])
            tmp_list.append(rec[3][1])
            tmp_list.append(rec[4][0])
            tmp_list.append(rec[4][1])
            tmp_list.append(rec[5][0])
            tmp_list.append(rec[5][1])
            twse_list.append(tmp_list)
            fin.extend(tmp_list)    
    print (lno(),twse_list)    
    sr_twse_list=sorted(twse_list, key=getKey,reverse=True)
    #for i in sr_twse_list:
    #print i[0],"W",(i[1]+i[3])/2,"L",(i[2]+i[5])/2,"W",(i[7]+i[9])/2,"L",(i[8]+i[11])/2
    with open(fpath, 'w',encoding='utf8',newline='') as csv_file:
        output = csv.writer(csv_file)
        for i in sr_twse_list:
            output.writerow(i)
    return fin
    
def time64_Date(x):
    ts = pd.to_datetime(str(x)) 
    d = ts.strftime('%y%m%d')
    return d    
def time64_DateTime(x):
    ts = pd.to_datetime(str(x)) 
    return ts 
    
def get_stock_df_bydate_nums(stock_no,nums,date):
    dstpath='%s/stock_data/%s.csv'%(datafolder(),stock_no)
    if debug==1:
        print (lno(),dstpath)
    #df = pd.read_csv(dstpath,encoding = 'big5')
    df = pd.read_csv(dstpath,encoding = 'utf-8')
        
    df.columns = ['date', 'vol', 'cash', 'open', 'high','low','close','diff','Tnumber','stock_name']
    outcols=['date','open', 'high','low','close','diff','vol']
    df=df.replace('--',np.NaN)
    df=df.replace('---',np.NaN)
    df.fillna(method='ffill',inplace=True)
    df=df.reset_index(drop=True)    
    lendf=len(df)
    #print  (lno(),df.tail(5))
    #print  (lno(),stock_no,date,lendf)  
    try :
        outdf = pd.DataFrame(np.empty(( lendf, len(outcols))) * np.nan, columns = outcols)
    except: 
        print  (lno(),stock_no,date,lendf)      
    #tokline_type(df)
    if len(df)<=10:
        return outdf
    if debug==1:
        print (lno(),df)
        print (lno(),df.loc[0])
    outdf.loc[0] = df.loc[0]
    j=0
    
    for i in range (lendf-1,-1,-1):
        #print (lno(),df.at[i,'date'])
        if twdate2datetime64(df.at[i,'date'])<=date :
            outdf.at[j,'date'] = twdate2datetime64(df.at[i,'date'])
            outdf.at[j,'vol'] = int( df.at[i,'vol'])
            if ( df.at[i,'diff']=='--' or  df.at[i,'diff']=='---'):
                outdf.at[j,'diff'] = 0.0
            else :    
                outdf.at[j,'diff'] = float( df.at[i,'diff'])
            """
            if ( df.at[i,'open']=='--' or  df.at[i,'open']=='---') :
                continue
            """
            if ( df.at[i,'open']=='--' or  df.at[i,'open']=='---') and (i>=1) :
                try :
                    outdf.at[j,'open']=float(df.at[i-1,'open'])
                    outdf.at[j,'high']=float(df.at[i-1,'high'])
                    outdf.at[j,'low']=float(df.at[i-1,'low'])
                    outdf.at[j,'close']=float(df.at[i-1,'close'])
                except :
                    #print (lno(),df.at[i-1,'open'])
                    outdf.at[j,'open']=np.nan
                    outdf.at[j,'high']=np.nan
                    outdf.at[j,'low']=np.nan
                    outdf.at[j,'close']=np.nan
    
            else :
                if ( df.at[i,'open']=='--' or  df.at[i,'open']=='---') and (i==0) :
                    print (lno(),outdf.at[j,'open'])
                else :
                    try :    
                        outdf.at[j,'open']=float(df.at[i,'open'])
                        outdf.at[j,'high']=float(df.at[i,'high'])
                        outdf.at[j,'low']=float(df.at[i,'low'])
                        outdf.at[j,'close']=float(df.at[i,'close'])
                    except :
                        #print (lno(),outdf.at[j,'open'])      
                        outdf.at[j,'open']=np.nan
                        outdf.at[j,'high']=np.nan
                        outdf.at[j,'low']=np.nan
                        outdf.at[j,'close']=np.nan
            #""""
            j=j+1
            if j>nums+10:
                break
    outdf.fillna(method='ffill')                
    outdf=outdf.dropna(how='any',axis=0)
    outdf=outdf.sort_values(by='date', ascending=True)
    outdf=outdf.reset_index(drop=True)
    if nums >0:
        outdf=outdf.tail(nums)
    try:
        outdf['open']=outdf['open'].astype('float64')
        outdf['high']=outdf['high'].astype('float64')
        outdf['low']=outdf['low'].astype('float64')
        outdf['close']=outdf['close'].astype('float64')
    except :
        print (lno(),outdf.tail(5))     
    #print (lno(),outdf.dtypes,outdf.tail(2))
    #df2['b_pwr'] = df2.apply(calc_bpwr, axis=1)
    #df2['s_pwr'] = df2.apply(calc_spwr, axis=1)
    return outdf  
def str_Ymd2md(x):
    ts = pd.to_datetime(str(x)) 
    d = ts.strftime('%m%d')
    return d 
def date_sub2time64(x):

    if '/' in x:
        tmp_list=x.split ('/')
        tmp_str="%d-%0s-%0s"%(int(tmp_list[0]),tmp_list[1],tmp_list[2])
        fin=datetime.strptime(tmp_str,'%Y-%m-%d')
    else:    
        tmp_list=x.split ('-')
        tmp_str="%d-%0s-%0s"%(int(tmp_list[0]),tmp_list[1],tmp_list[2])
        fin=datetime.strptime(tmp_str,'%Y-%m-%d')
    return np.datetime64(fin)     
def stock_is_otc(stock_id,selday):
    filename ='csv/data/otc/{}'.format(selday.strftime('%Y%m%d'))
    #if debug==1:
    #    print (lno(),filename)
    if os.path.exists(filename):
        df_s = pd.read_csv(filename,encoding = 'utf-8',dtype={'stock_id': 'str'})
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        df= df_s[(df_s['stock_id'] == stock_id)]
        if len(df)==1:
            return 1
            
    return 0
def get_name_by_stock_id(stock_id):
    filename='csv/twse_list.csv'
    if os.path.exists(filename):
        df_s = pd.read_csv(filename,encoding = 'utf-8-sig',header=None,names=['no','name','d1','d2'],dtype= {'no':str, 'name': str,'d1':str, 'd2': str})
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        df= df_s[(df_s['no'] == stock_id)]
        if len(df)==1:
            return df['name'].values.tolist()[0]
    filename='csv/otc_list.csv'
    if os.path.exists(filename):
        df_s = pd.read_csv(filename,encoding = 'utf-8-sig',header=None,names=['no','name','d1','d2'],dtype= {'no':str, 'name': str,'d1':str, 'd2': str})
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        df= df_s[(df_s['no'] == stock_id)]
        if len(df)==1:
            return df['name'].values.tolist()[0]
            
            
    return 0   
def datafolder():
    if platform.system().upper()=='LINUX':
        #print(lno(),platform.system())
        return '/src/stock/data'
    else :
       # print(lno(),platform.system())
        return 'h:\data'
        #return 'd:\data'

def time64_Date_str(date):
    ts = pd.to_datetime(str(date)) 
    #print(lno(),ts)
    d = ts.strftime('%Y-%m-%d')
    return d

            
def get_tse_exchange_data(selday,ver=0,debug=0):
    #"""
    if ver==1:
        try:
            exc=exchange_data('tse')
            df=exc.get_df(selday)
            if len(df):
                df['date']=selday
                return df
            else:
                return pd.DataFrame()    
        except:
            pass
    #date_str = selday.strftime('%Y/%m/%d')
    #"""
    filename ='{}/exchange/tse/{}'.format(datafolder(),selday.strftime('%Y%m%d'))
    dtypes= {'stock_id': 'str','vol':'str', 'cash': 'str','open':'str', 'high': 'str','low':'str', 'close': 'str','diff':'str'}  
    if debug==1:
        print (lno(),filename)
    if os.path.exists(filename):
        df_s = pd.read_csv(filename,encoding = 'utf-8',dtype=dtypes)
        df_s.dropna(axis=1,how='all',inplace=True)
        #df_s.dropna(inplace=True)
        df_s['date']=selday
        if debug==1:
            print (lno(),df_s)
        return df_s.reset_index(drop=True).copy()   
    else:
        if debug==1:    
            print (lno(),filename,"not exit pls check")
        return pd.DataFrame()
    
def get_otc_exchange_data(selday,ver=0,debug=0):
    #date_str = selday.strftime('%Y/%m/%d')
    #"""
    if ver==1:
        try:
            exc=exchange_data('otc')
            df=exc.get_df(selday)
            if len(df):
                df['date']=selday
                return df
        except:
            pass
    #"""    
    #filename ='csv/data/otc/{}'.format(selday.strftime('%Y%m%d'))
    dtypes= {'stock_id': 'str','vol':'str', 'cash': 'str','open':'str', 'high': 'str','low':'str', 'close': 'str','diff':'str'}  
    filename ='{}/exchange/otc/{}'.format(datafolder(),selday.strftime('%Y%m%d'))
    if debug==1:
        print (lno(),filename)
    if os.path.exists(filename):
        df_s = pd.read_csv(filename,encoding = 'utf-8',dtype=dtypes)
        df_s.dropna(axis=1,how='all',inplace=True)
        #df_s.dropna(inplace=True)
        df_s['date']=selday
        if debug==1:
            print (lno(),df_s)
        return df_s.reset_index(drop=True).copy()       
    else:
        print (lno(),filename,"not exit pls check")
        return pd.DataFrame()  
def check_work_date(selday):
    filename ='csv/data/tse/{}'.format(selday.strftime('%Y%m%d'))
    if os.path.exists(filename):
        return True
    return False    

def check_dst_folder(dstpath):
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath)     
        
def get_last_valley(df,rundate,ddd=0):
    if ddd==1:
        print(lno(),df[(df.loc[:,"date"] <= np.datetime64(rundate))]['low'])
    df1=df[(df.loc[:,"date"] <= np.datetime64(rundate))].tail(30)    
    valleys = signal.find_peaks(-df1[(df1.loc[:,"date"] <= np.datetime64(rundate))]['low'], distance=2)[0]
    if len (valleys)!=0:
        return df.iloc[valleys[-1]]['low']
    return None

def get_last_peak(df,rundate,ddd=0):
    if ddd==1:
        print(lno(),df[(df.loc[:,"date"] <= np.datetime64(rundate))]['high'])
    df1=df[(df.loc[:,"date"] <= np.datetime64(rundate))].tail(30)
    
    peaks = signal.find_peaks(df1[(df1.loc[:,"date"] <= np.datetime64(rundate))]['high'], distance=2)[0]
    if len (peaks)!=0:
        return df.iloc[peaks[-1]]['high']
    return None    
def calc_profit(buy_price,sell_price):
    transaction_tax=0.001425*0.5
    tax_payment=0.003
    fix_buy_price=buy_price*(1+transaction_tax)
    fix_sell_price=sell_price*(1-tax_payment-transaction_tax)
    return fix_sell_price/fix_buy_price    
def check_stock_id(stock_id):
    if len(stock_id)!=4:
        return False
    if stock_id[0:1]=='0':
        return False
    return True    
from sqlalchemy.types import NVARCHAR, Float, Integer    
from sqlalchemy import Table, Column,  String, MetaData, ForeignKey    
class exchange_data:
    def __init__(self,market):
        ##https://www.jishuwen.com/d/pbqG/zh-tw
        folder=datafolder()
        check_dst_folder(folder)
        if platform.system().upper()=='LINUX':
            DB_CONNECT_STRING = 'sqlite:///sql/{}_exchange_data.db'.format(market)
        else:
            DB_CONNECT_STRING = 'sqlite:///sql/{}_exchange_data.db'.format(market)    
        #self.engine = create_engine(DB_CONNECT_STRING, echo=True)
        self.engine = create_engine(DB_CONNECT_STRING, echo=False)
        self.market=market
        self.con = self.engine.connect()
        self.dtypedict = {
            'stock_id': NVARCHAR(length=10),
            'open': Float(),
            'high': Float(),
            'low': Float(),
            'close': Float(),
            'diff': Float(),
            'vol': Integer(),
            'cash': Integer(),
            'stock_name': NVARCHAR(length=20)
            }

        pass
    def save_sql(self,selday):
        if self.market=='tse':
            #print(lno())
            df=get_tse_exchange_data(selday)
        elif self.market=='otc':
            #print(lno())
            df=get_otc_exchange_data(selday)
        else:
            print(lno(),'unknown market')    
            return
        #"""    
        
            #print(lno(),df.head())
        #"""
        #print(lno(),df.head(),df.dtypes)
        if len(df):
            """
            def strdate(date):
                #return '%d-%02d-%02d'%(date.year,date.month,date.day)
                #print(lno(),date)
                #print(lno(),str(date))
                ts = pd.to_datetime(str(date)) 
                #print(lno(),ts)
                d = ts.strftime('%Y-%m-%d')
                return d
            df['date']=df['date'].apply(strdate)
            """
            df=df.replace('--',np.nan)
            df=df.replace('---',np.nan)
            df=df.replace('----',np.nan)
            df=df.replace('除息','0')
            df=df.replace('除權','0')
            table_name='%d%02d%02d'%(selday.year,selday.month,selday.day)
            df.to_sql(name=table_name, con=self.con, if_exists='replace', index=False,dtype=self.dtypedict,chunksize=10)
            #df.tosql
    def get_df(self,selday):   
        #print(lno(),self.stock_id) 
        #table_name=(selday.strftime('%Y%m%d'))
        #table_names = self.engine.table_names() # 取得資料庫內全部Tables的名稱
        #print(lno(),table_names)  
        #if table_name in table_names:
        try:
            df = pd.read_sql('select * from "{}"'.format(selday.strftime('%Y%m%d')), con=self.con)
            #df=df.replace('-',np.NaN)
            #print(lno(),self.df)
            return df
        except:
            return pd.DataFrame()
    def get_df_date_parse(self,selday):   
        #print(lno(),self.stock_id) 
        #table_name=(selday.strftime('%Y%m%d'))
        #table_names = self.engine.table_names() # 取得資料庫內全部Tables的名稱
        #print(lno(),table_names)  
        #if table_name in table_names:
        try:
            df = pd.read_sql('select * from "{}"'.format(selday.strftime('%Y%m%d')), con=self.con, parse_dates=['date'])
            #df=df.replace('-',np.NaN)
            #print(lno(),self.df)
            return df
        except:
            return pd.DataFrame()    
    def get_last_df_bydate(self,selday):   
        #print(lno(),self.stock_id) 
        
        table_names = self.engine.table_names() # 取得資料庫內全部Tables的名稱
        print(lno(),table_names)  
        nowdate=selday
        while True:
            table_name=(nowdate.strftime('%Y%m%d'))
            if table_name in table_names:
                break
            nowdate=nowdate-relativedelta(days=1)
                
        try:
            df = pd.read_sql('select * from "{}"'.format(table_name), con=self.con, parse_dates=['date'])
            #df=df.replace('-',np.NaN)
            #print(lno(),self.df)
            return df
        except:
            return pd.DataFrame()      
    def close(self):
        self.con.close()
def exchange2sql(startdate,enddate):
    #"""
    now_date = startdate 
    while   now_date<=enddate :
        exc=exchange_data('tse')
        exc.save_sql(now_date)
        #df=exc.get_df(now_date)
        #exc.close()
        print(lno(),now_date)
        now_date = now_date + relativedelta(days=1)
    #"""    
    now_date = startdate 
    while   now_date<=enddate :    

        exc=exchange_data('otc')
        exc.save_sql(now_date)
        #df=exc.get_df(now_date)
        #print(lno(),df.head())
        print(lno(),now_date)
        now_date = now_date + relativedelta(days=1)
    #"""
def get_tse_exchange_data_sql(date,exc=None):
    if exc==None:
        exc1=exchange_data('tse')
        exc1.get_df(date)
    else:
        exc.get_df(date)        

class stock_data:
    def __init__(self,engine=None):
        ##https://www.jishuwen.com/d/pbqG/zh-tw
        if engine==None:
            self.engine = create_engine('sqlite:///sql/stock_data.db', echo=False)
            #self.engine = create_engine( r'sqlite:///d:\data\stock_data.db', echo=False)
        else:
            self.engine=engine
        #self.stock_id=stock_id
        self.datafolder='%s/stock_data'%(datafolder())
        self.con = self.engine.connect()
        #self.df=pd.DataFrame()
        """
        self.dtypedict = {
            #'stock_id': NVARCHAR(length=10),
            'open':  NVARCHAR(length=10),
            'high': NVARCHAR(length=10),
            'low':  NVARCHAR(length=10),
            'close':NVARCHAR(length=10),
            'diff': NVARCHAR(length=10),
            'stock_name': NVARCHAR(length=20)
            }
        """
        self.dtypedict = {
            'stock_id': NVARCHAR(length=10),
            'open': Float(),
            'high': Float(),
            'low':  Float(),
            'close':Float(),
            'diff': Float(),
            'vol': Integer(),
            'stock_name': NVARCHAR(length=20)
            }
        #date      vol   open   high    low  close  diff stock_name
        pass
    def csvtosql(self,stock_id):    
        #print(lno())
        #file_name='{}/{}.csv'.format(self.datafolder,stock_id)
        #dtypes= {'vol':np.int64, 'cash': np.int64,'open':np.float64, 'high': np.float64,'low':np.float64, 'close': np.float64,'diff':np.float64}  
        #dateparse = lambda dates: pd.datetime.strptime(dates,'%Y-%m-%d')
        #df = pd.read_csv(file_name,encoding = 'utf-8',parse_dates=['date'], date_parser=dateparse,dtype=dtypes)
        #df = pd.read_csv('{}/{}'.format(self.datafolder, file_name),encoding = 'big5', header=1, index_col=0, usecols=[0,1,2,3], dtype={0:str, 1:np.int64, 2:np.int64, 3:np.int64}, skiprows=range(8, 50), thousands=',') # 依格式讀取資料
        df=get_stock_df(stock_id)
        if len(df):
            #print(lno(),df.head())
            #raise
            """
            def strdate(date):
                return '%d-%02d-%02d'%(date.year,date.month,date.day)
            df['date']=df['date'].apply(strdate)
            """
            df.to_sql(name=stock_id, con=self.con, if_exists='replace', index=False,dtype=self.dtypedict,chunksize=10)

    def insertdf(self,df):
        if len(df):
            date_xx= df.reset_index().at[0,'date']
            #print(lno(),date_xx,type(date_xx))
            if  (type(date_xx) is datetime) or (type(date_xx) is pd.Timestamp):
                date=date_xx
            elif type(date_xx)==str:
                date=datetime.strptime(date_xx,'%Y%m%d')
                print(lno(),date_xx,type(date_xx))
            else:
                print(lno(),date_xx,type(date_xx))    
                raise
            enddate= date + relativedelta(days=1)    
            stock_id= df.reset_index().at[0,'stock_id']
            #print(lno(),date_str,df['stock_id'].values.tolist())
            cmd='SELECT * FROM "{}" WHERE date >= "{}" and date < "{}"'.format(stock_id,date,enddate)
            try:
                df_sql=pd.read_sql(cmd, con=self.con)  
                if len(df_sql)!=0:
                    repeat=1
                else:
                    repeat=0    
            except:
                if stock_id not in self.engine.table_names():
                    repeat=0
                else:
                    print(lno(),stock_id,date,enddate)
                    raise    
                    
            if repeat==0:
                #print(lno(),df_sql)
                df=df.drop(['stock_id'], axis=1)
                #print(lno(),df.head())
                #df['date']=df['date'].apply(time64_Date_str)
                #df.to_sql(name=self.stock_id, con=self.con, if_exists='append', index=False,dtype=self.dtypedict,method='upsert_ignore')        
                df.to_sql(name=stock_id, con=self.con, if_exists='append', index=False,dtype=self.dtypedict)        
                pass
            else:
                print(lno(),'repeat')
        

    def showtable(self):
        cmd='SELECT * FROM "{}" WHERE date IS "{}"'.format(self.stock_id,'2019-10-15')
        df=pd.read_sql(cmd, con=self.con)  
        print(lno(),df)
        #table_names_1 = self.engine.table_names() # 取得資料庫內全部Tables的名稱
        #print(table_names_1)    
        #df_mysql = pd.read_sql('select * from test', con=self.con)  
        #print(lno(),df_mysql)  
    def get_df(self,stock_id):
        try:
            #df = pd.read_sql('select * from "{}" where date < "{}" and date >= "{}"'.format(stock_id,date,date1), con=self.con,index_col='date', parse_dates=['date'])
            #date=datetime(2019,10,18) 
            #df = pd.read_sql('select * from "{}" where date < "{}" ORDER BY date DESC'.format(stock_id,date), con=self.con, parse_dates=['date'])
            df = pd.read_sql('select * from "{}"'.format(stock_id), con=self.con, parse_dates=['date'])
            #df=pd.read_sql(stock_id, self.engine, parse_dates=['date'])  
            df=df.fillna(method='ffill')
            #print(lno(),df.head())
            #df['date']=df['date'].apply(date_sub2time64)
            return df
        except:
            return pd.DataFrame()
    def get_df_by_date_day(self,stock_id,date,day):
        try:
            #df = pd.read_sql('select * from "{}" where date < "{}" and date >= "{}"'.format(stock_id,date,date1), con=self.con,index_col='date', parse_dates=['date'])
            #date=datetime(2019,10,18) 
            #df = pd.read_sql('select * from "{}" where date < "{}" ORDER BY date DESC'.format(stock_id,date), con=self.con, parse_dates=['date'])
            startdate= date - relativedelta(days=day+10)  
            enddate= date + relativedelta(days=1)  
            #print(lno(),startdate,date)  
            cmd='SELECT * FROM "{}" WHERE date >= "{}" and date < "{}"'.format(stock_id,startdate,enddate)
            df = pd.read_sql(cmd, con=self.con, parse_dates=['date'])
            #df=pd.read_sql(stock_id, self.engine, parse_dates=['date'])  
            df=df.fillna(method='ffill')
            #print(lno(),df.head())
            #df['date']=df['date'].apply(date_sub2time64)
            return df
        except:
            return pd.DataFrame() 
    def get_df_by_enddate_num(self,stock_id,date,num):
        try:
            #df = pd.read_sql('select * from "{}" where date < "{}" and date >= "{}"'.format(stock_id,date,date1), con=self.con,index_col='date', parse_dates=['date'])
            #print(lno(),stock_id,date,num)
            #print(lno(),type(date))
            stardate=date- relativedelta(days=num*2) 
            #print(lno(),stock_id,date,num)
            cmd='SELECT * FROM "{}" WHERE date >= "{}" and date < "{}" '.format(stock_id,stardate,date+relativedelta(days=1))
            #print(lno(),cmd)
            #cmd='SELECT * FROM "{}" WHERE date <= "{}" ORDER BY "date" DESC limit {} '.format(stock_id,date,num)
            df = pd.read_sql(cmd, con=self.con, parse_dates=['date'])
            #df=df.sort_values(by=['date'], ascending=True).reset_index(drop=True)
            df=df.fillna(method='ffill')
            df=df.tail(num).reset_index(drop=True)
            #print(lno(),df.head())
            return df
        except:
            return pd.DataFrame()                 
    def get_df_by_startdate_enddate(self,stock_id,startdate,enddate):
        try:
            #print(lno(),startdate,date)  
            cmd='SELECT * FROM "{}" WHERE date >= "{}" and date < "{}"'.format(stock_id,startdate,enddate)
            df = pd.read_sql(cmd, con=self.con, parse_dates=['date'])
            #df=pd.read_sql(stock_id, self.engine, parse_dates=['date'])  
            df=df.fillna(method='ffill')
            #print(lno(),df.head())
            #df['date']=df['date'].apply(date_sub2time64)
            return df
        except:
            return pd.DataFrame()        
    def finish(self):
        self.con.close()
def insert_daily_stock_data(startdate):
    tStart = time.time()
    df=get_tse_exchange_data(startdate)
    #print(lno(),startdate,type(startdate))
    #df['date']
    stk=stock_data()
    for i in range (0,len(df)):
        df1=df[i:i+1]
        #print(lno(),df1.dtypes)
        if len(df.at[i,'stock_id'])!=4:
            continue
        #print(lno(),df1)
        #stk=stock_data(df.at[i,'stock_id'],engine=engine)
        #dfo=stk.get_df(df.at[i,'stock_id'])
        #print(lno(),dfo.head(),dfo.dtypes)
        stk.insertdf(df1)
        #stk.csvtosql()
        #df1=stk.get_df()
    df=get_otc_exchange_data(startdate)
    for i in range (0,len(df)):
        if len(df.at[i,'stock_id'])!=4:
            continue
        df1=df[i:i+1]
        #print(lno(),df1)
        #stk=stock_data(df.at[i,'stock_id'],engine=engine)
        stk.insertdf(df1)
    tEnd = time.time()      
    print ("It cost %.3f sec" % (tEnd - tStart))   
def to_html(df,filen):
    check_dst_folder(os.path.dirname(filen))
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)
    df.to_html(filen,escape=False,index=False,sparsify=True,border=2,index_names=False)
    pd.set_option('display.max_colwidth', old_width) 
    
def get_stock_sql_engine(stock_id):
    check_dst_folder('sql/stock')    
    engine = create_engine('sqlite:///sql/stock/{}.db'.format(stock_id), echo=False)
    return engine

def stock_df_to_sql(stock_id,table_name,df):
    engine=get_stock_sql_engine(stock_id)
    con = engine.connect()
    df.to_sql(name=table_name, con=con, if_exists='replace',  index= False,chunksize=10) 
    
def stock_df_to_sql_append(stock_id,table_name,df):
    engine=get_stock_sql_engine(stock_id)
    con = engine.connect()
    if table_name in engine.table_names():
        cmd='SELECT * FROM "{}" WHERE ys == "{}" '.format(table_name,df.iloc[0]['ys'])
        df_query= pd.read_sql(cmd, con=con)
        if len(df_query):
            print(lno(),"repeat",stock_id,df.iloc[0]['ys']/4,df.iloc[0]['ys']%4)
            return
        else:
            #print(lno(),df)
            #print(lno(),df.columns)
            df.to_sql(name=table_name,  con=con, if_exists='append',  index= False,chunksize=10)
    else:    
        df.to_sql(name=table_name, con=con, if_exists='replace',  index= False,chunksize=10) 
def stock_read_sql_add_df(stock_id,table_name,df):

    engine=get_stock_sql_engine(stock_id)
    con = engine.connect()
    if table_name in engine.table_names():
        cmd='SELECT * FROM "{}"'.format(table_name)
        df_query= pd.read_sql(cmd, con=con, parse_dates=['date'])
        if df.columns.all()==df_query.columns.all():
            df=df.append(df_query,ignore_index=True)
            df.drop_duplicates(subset=['date'], keep='first', inplace=True)
            df=df.sort_values(by=['date'], ascending=True)
        print(lno(),df)    
        #raise
        df.to_sql(name=table_name,  con=con, if_exists='replace',  index= False,chunksize=10)
    else:    
        df.to_sql(name=table_name, con=con, if_exists='replace',  index= False,chunksize=10)         
def stock_df_to_sql_append_querydate(stock_id,table_name,df):
    engine=get_stock_sql_engine(stock_id)
    con = engine.connect()
    if table_name in engine.table_names():
        #print(lno(),df.iloc[0])   
        date=df.iloc[0]['date']
        cmd='SELECT * FROM "{}" WHERE date >= "{}" and date < "{}" '.format(table_name,date-relativedelta(days=1),date+relativedelta(days=1))
        df_query= pd.read_sql(cmd, con=con)
        if len(df_query):
            print(lno(),"repeat",stock_id,df.iloc[0]['date'])
            return
        else:
            #print(lno(),df)
            #print(lno(),df.columns)
            df.to_sql(name=table_name,  con=con, if_exists='append', index= False,dtype={'date': Date()}, chunksize=10)
    else:
        #print(lno(),df.iloc[0])    
        df.to_sql(name=table_name, con=con, if_exists='replace',  index= False,dtype={'date': Date()},chunksize=10)        
def tofloat64(x):
    if type(x)==str and '-' == x:
        return np.NaN
    if type(x)==str and '--' == x:
        return np.NaN
    return float(x)
def get_sql_stock_df(stock_id,table_name,debug=0):
    engine=get_stock_sql_engine(stock_id)
    con = engine.connect()
    try:
        cmd='SELECT * FROM "{}" '.format(table_name)
        d= pd.read_sql(cmd, con=con)
    except:
        print(lno(),stock_id,table_name,"NG")    
        return pd.DataFrame()
    d.replace('-',np.NaN)
    d.replace('--',np.NaN)
    if table_name=='mix_income':
        #d['營業收入']=d['營業收入'].astype('float64')
        if debug==1:
            print(lno(),d)
        d['營業收入']=d['營業收入'].apply(tofloat64)
        d['營業毛利（毛損）淨額']= d['營業毛利（毛損）淨額'].apply(tofloat64)
        if debug==1:
            print(lno(),d)
    return d
    
    
             
import director

def get_director_df(stock_id,dw=1,debug=1):
    df=director.get_stock_director_df_goodinfo(stock_id,download=dw)
    return df
import eps
def get_stock_last_year_income(date,stock_id):
    nowdate=date
    get_data=False
    df_s=pd.DataFrame()
    cnt=0
    while get_data==False:
        year=str(int(nowdate.year)-1911)
        season=4
        file='data/eps/tse_%s-%s.html'%(year, season)
        if os.path.exists(file):  
            df_s = pd.read_html(file,encoding = 'utf8')
            for df in df_s:
                if '公司代號' in df.columns:
                    df['公司代號']=df['公司代號'].astype(str)
                    #print(lno(),df.dtypes)
                    d=df.loc[df['公司代號'] == stock_id]
                    if len(d)!=0:
                        return d

            
            file='data/eps/otc_%s-%s.html'%(year, season)
            if os.path.exists(file):  
                df_s = pd.read_html(file,encoding = 'utf8')
                for df in df_s:
                    if '公司代號' in df.columns:
                        df['公司代號']=df['公司代號'].astype(str)
                        #print(lno(),df.dtypes)
                        d=df.loc[df['公司代號'] == stock_id]
                        if len(d)!=0:
                            return d
            
        else:    
            nowdate = nowdate - relativedelta(years=1)
            cnt=cnt+1
        if cnt>=3:
            break
    return pd.DataFrame()  

def get_stock_pe_networth_yield_df(r):
    dst_folder='data/down_pe_networth_yield'
    print(lno(),r.market)
    if 'tse' ==r.market:
        file='%s/tse%s.csv'%(dst_folder,r.date.strftime('%Y%m%d'))
    else:
        file='%s/otc%s.csv'%(dst_folder,r.date.strftime('%Y%m%d'))
    try:    
        df = pd.read_csv(file,encoding = 'utf-8',dtype= {'stock_id':str})
    except:
        print(lno(),file,"ng file")
        return pd.DataFrame()     
    #print(lno(),r.stock_id)
    #print(lno(),df.dtypes)
    #print(lno(),df[df['stock_id']==r.stock_id])
    return df[df['stock_id']==r.stock_id]
    """
    try:
        df=get_sql_stock_df(r.stock_id,"pe_ratio")
    except:
        print(lno(),r)
        raise    
    return df
    """
def get_stock_revenue_df(r):
    df=get_sql_stock_df(r.stock_id,"revenue")
    d=df.sort_values(by='date',ascending=False).reset_index(drop=True)
    return d
def get_stock_industry_status_df(r):
    dst_folder='xq_data'
    #print(lno(),r.market)
    if 'tse' ==r.market:
        file='%s/tse.csv'%(dst_folder)
    else:
        file='%s/otc.csv'%(dst_folder)
    
    try:    
        df = pd.read_csv(file,encoding = 'big5hkscs',dtype= {'代碼':str})
        if len(df.columns)==6:
            df.columns=['stock_id', 'stock_name', '產業地位', '產業', '細產業', '市值']
            df=df[['stock_id', 'stock_name', '產業地位', '產業', '細產業']]
            #print(lno(),df.iloc[0])
        elif len(df.columns)==5:    
            df.columns=['stock_id', 'stock_name', '產業地位', '產業', '細產業']
        else:    
            print(lno(),df.columns,len(df.columns))    
            raise
    except:
        print(lno(),file,"ng file")
        return pd.DataFrame()     
    #print(lno(),type(r.stock_id))
    #print(lno(),df.dtypes)
    #print(lno(),df[df['stock_id']=='1101'])
    return df[df['stock_id']==r.stock_id]
    
#抓取資料最新月的累計營收    
def get_stock_cumulative_revenue(r):
    df=get_sql_stock_df(r.stock_id,"revenue")
    d=df.sort_values(by='date',ascending=False).reset_index(drop=True)
    
    print(lno(),d.head(1))
    return d.head(1)
def get_stock_director_df(r):
    df=get_sql_stock_df(r.stock_id,"director")
    print(lno(),df)
    return df

def get_stock_tdcc_dist_df(r):
    tdcc=get_tdcc_dist() 
    df=tdcc.get_df(r.stock_id)
    return df
      
def get_stock_season_df(r,debug=0):
    df=get_sql_stock_df(r.stock_id,"mix_income")
    if len(df)==0:
        print(lno(),r.stock_id,"no mix_income" )
        return pd.DataFrame()
    #print(lno(),df)
    d=df.sort_values(by='ys',ascending=False).reset_index(drop=True)
    if debug==1:
        print(lno(),d)
    #單位K
    d.replace('-',np.NaN)
    try:
        d['單季營收']=d['營業收入']-d['營業收入'].shift(-1)
        d['單季EPS']=d['基本每股盈餘（元）']-d['基本每股盈餘（元）'].shift(-1)
        d['單季毛利淨額']=d['營業毛利（毛損）淨額']-d['營業毛利（毛損）淨額'].shift(-1)
        d['單季營業利益淨額']=d['營業利益（損失）']-d['營業利益（損失）'].shift(-1)
        d['單季綜合損益總額']=d['本期綜合損益總額']-d['本期綜合損益總額'].shift(-1)
    except:
        print(lno(),d)
        print(lno(),d.iloc[0])
        raise    
    #d1=d.head(8).copy()
    #d1=d
    d1=pd.DataFrame()
    d['year']=(d['ys']/4)+1911
    d['year']=d['year'].astype('int')
    d['season']=d['ys']%4 +1
    for i in range(0,len(d)):
        if d.iloc[i]['ys']%4==0:
            d.loc[i,'單季營收']= d.iloc[i]['營業收入']
            d.loc[i,'單季EPS']=d.iloc[i]['基本每股盈餘（元）']
            d.loc[i,'單季毛利淨額']=d.iloc[i]['營業毛利（毛損）淨額']
            d.loc[i,'單季營業利益淨額']=d.iloc[i]['營業利益（損失）']
            d.loc[i,'單季綜合損益總額']=d.iloc[i]['本期綜合損益總額']
    #print(lno(),d.iloc[0])
    #print(lno(),d)
    return d
   
import revenue
def get_stock_RD_fee(stock_id,dw,debug=1,ver=1):
    #reurn unit 百萬
    if ver==1:
        df=get_sql_stock_df(stock_id,"RD_fee")
        print(lno(),df)
        if len(df)>=4:
            d=df.head(4).copy()
            d['研發費用(百萬)']=d['研發費用(百萬)'].astype('float64')
            if debug==1:
                print(lno(),d)
            return d['研發費用(百萬)'].sum()
        return np.NaN
    df=revenue.down_stock_composite_income(stock_id,download=dw)
    if len(df)>=4:
        d=df.head(4).copy()
        d['研究發展費']=d['研究發展費'].astype('float64')
        if debug==1:
            print(lno(),d)
        return d['研究發展費'].sum()
    return np.NaN
import tdcc_dist
g_tdcc=None
def get_tdcc_dist():
    global g_tdcc
    if g_tdcc==None:
        g_tdcc=tdcc_dist.tdcc_dist()
    return g_tdcc 

def get_total_stock_num(stock_id,date):
    tdcc=get_tdcc_dist() 
    total_stock_nums=tdcc.get_total_stock_num(stock_id,date)
    return total_stock_nums
g_stk = None
def get_stock_data():
    global g_stk
    if g_stk==None:
        print(lno(),'111')
        g_stk=stock_data()
    return g_stk

def get_stock_last_close(stock_id,date):
    stk=get_stock_data()    
    df=stk.get_df_by_startdate_enddate(stock_id,date-relativedelta(days=14),date+relativedelta(days=1))  
    if len(df.index)==0:
        return np.NaN
    return df.iloc[-1]['close']
def get_tse_otc_stock_df_by_date(date):
    date1=date
    while True:
        d1=exchange_data('tse').get_df_date_parse(date1)
        if len(d1)==0:
            date1=date1-relativedelta(days=1)
            continue
        else:
            d2=exchange_data('otc').get_df_date_parse(date1)
            d3=pd.concat([d1,d2])
            
            break
    return d3
    
if __name__ == '__main__':
    if sys.argv[1]=='exc_sql' :
        print(lno(),'convert exchange(csv/data/tse/zzz) to sql database(data/xxx_exchange_data.db)')
        if len(sys.argv)==4 :
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            exchange2sql(startdate,enddate)
    elif sys.argv[1]=='stock_sql' :

        print(lno(),'convert stock(data/xxx) to sql database(data/stock_data.db)')
        if len(sys.argv)!=3 :
            print(lno(),'fun t2 start ')
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        tStart = time.time()
        stk=stock_data()
        tse_df=get_tse_exchange_data(startdate)
        if len(tse_df)!=0:
            list_=tse_df['stock_id'].values.tolist()
            for i in list_:
                if len(i)!=4:
                    continue
                print(lno(),i)
                
                stk.csvtosql(i)
        otc_df=get_otc_exchange_data(startdate)
        if len(otc_df)!=0:
            list_=otc_df['stock_id'].values.tolist()
            for i in list_:
                if len(i)!=4:
                    continue
                print(lno(),i)
               
                stk.csvtosql(i)        
        tEnd = time.time()
        print ("It cost %.3f sec" % (tEnd - tStart))   
    elif sys.argv[1]=='t3' :
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        tStart = time.time()
        stk=stock_data()
        df=get_tse_exchange_data(startdate)
        #print(lno(),df.head(),df.dtypes)
        #df['date']
        for i in range (0,len(df)):
            df1=df[i:i+1]
            if len(df.at[i,'stock_id'])!=4:
                continue
            #print(lno(),df1)
            #dfo=stk.get_df()
            #print(lno(),dfo.head(),dfo.dtypes)
            stk.insertdf(df1)
            #stk.csvtosql()
            #df1=stk.get_df()
        df=get_otc_exchange_data(startdate)
        for i in range (0,len(df)):
            if len(df.at[i,'stock_id'])!=4:
                continue
            df1=df[i:i+1]
            #print(lno(),df1)
            stk.insertdf(df1)

        tEnd = time.time()      
        print ("It cost %.3f sec" % (tEnd - tStart))   
       

    elif sys.argv[1]=='t4' :
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        
        stk=stock_data()
        df=get_tse_exchange_data(startdate)
        tStart = time.time()
        for i in range (0,len(df)):
            stock_id=df.at[i,'stock_id']
            df1=df[i:i+1]
            if len(stock_id)!=4:
                continue
            #print(lno(),df1)
            #dfo=stk.get_df(stock_id)
            dfo=get_stock_df(stock_id)
            #print(lno(),dfo.head())
            if i>200:
                break

        tEnd = time.time()      
        print ("It cost %.3f sec" % (tEnd - tStart)) 
        tStart = time.time()
        for i in range (0,len(df)):
            stock_id=df.at[i,'stock_id']
            df1=df[i:i+1]
            if len(stock_id)!=4:
                continue
            #print(lno(),df1)
            dfo=stk.get_df(stock_id)
            #print(lno(),dfo.head())
            #dfo=get_stock_df(stock_id)
            if i>200:
                break
        tEnd = time.time()      
        print ("It cost %.3f sec" % (tEnd - tStart)) 
    elif sys.argv[1]=='t5' :
        enddate=datetime.strptime(sys.argv[2],'%Y%m%d')   
        stk=stock_data()
        df=get_tse_exchange_data(enddate)
        tStart = time.time()
        def test(r):
            dfx=stk.get_df_by_enddate_num(r.stock_id,enddate,60)    
        df.apply(test,axis=1)
        tEnd = time.time()      
        print ("It cost %.3f sec" % (tEnd - tStart)) 
        #df=stk.get_df('6152')
        #print(lno(),df)

            
            
