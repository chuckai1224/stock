# -*- coding: utf-8 -*-
#import grs
import csv
import os
import sys
import time
import powerinx
import kline

from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

import stock_big3
import stock_comm as comm
import tdcc_dist
#from stock_comm import TWSENo
#from stock_comm import OTCNo
#import kline

import numpy as np
import pandas as pd

import pyecharts

#from pyecharts.charts import Page
#from pyecharts.charts import Candlestick
#from pyecharts.charts import Kline, Bar, Line, Grid,EffectScatter
#from pyecharts import Overlap
#from pyecharts_snapshot.main import make_a_snapshot

import eps
import seaborn as sns
import matplotlib as mpl
import revenue
from inspect import currentframe, getframeinfo
#from PyQt4.QtGui import QTextDocument, QPrinter, QApplication
import pdfkit as pdf
from PIL import Image
debug=0
TSE_KLINE_PATH='csv/tse_kline'
out_path=''
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),currentframe().f_back.f_lineno)
def get_date():
    if len(sys.argv)==1:
        today= datetime.today().date()
        dataday=datetime(year=today.year,month=today.month,day=today.day,)
        print (dataday)
    else:   
        dataday=datetime.strptime(sys.argv[1],'%Y%m%d')
    return dataday
def check_dst_folder(dstpath):
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath)    
def get_TWSE_df():
    filen='csv/twse_list.csv'
    df = pd.read_csv(filen,encoding = 'utf-8-sig',header=None,names=['no','name','d1','d2'],dtype= {'no':str, 'name': str,'d1':str, 'd2': str})
    #df.columns = ['no','name','d1','d2']
    df.dropna(axis=1,how='all',inplace=True)
    #print (lno(),df.dtypes)
    return df
def get_over50_list():
    filen='csv/over50.csv'
    df = pd.read_csv(filen,encoding = 'utf-8-sig',header=None)
    df.columns = ['no','dd','dd1']
    df.dropna(axis=1,how='all',inplace=True)
    print (lno(),df,df.dtypes)
    return df['no'].values.tolist()    
def get_OTC_df():
    filen='csv/otc_list.csv'
    df = pd.read_csv(filen,encoding = 'utf-8-sig',header=None,names=['no','name','d1','d2'],dtype= {'no':str, 'name': str,'d1':str, 'd2': str})
    #df.columns = ['no','name','d1','d2']
    df.dropna(axis=1,how='all',inplace=True)
    #print (lno(),df.dtypes)
    return df
        
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

    if total_buy+total_sell==0 :
        #print (lno(),total_buy,total_sell)  
        #print (lno(),open,high,low,close,diff)
        return 0
    
    buy_pwr=vol*total_buy/(total_buy+total_sell)
    sell_pwr=vol*total_buy/(total_buy+total_sell)
    return buy_pwr
def kpwr_mode1_calc(row):
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
    pre_close=close-diff
    if open >pre_close:
        kpwr_mode1= (close -pre_close)/pre_close
    else :
        kpwr_mode1= (close -open)/pre_close    
    return kpwr_mode1    
def calc_spwr(row):
    return row['vol']-row['b_pwr']
## origin mode 
def check_mode_s1(flag,stock_no,enddate):
    df1=get_stock_df_bydate_nums(stock_no,240,enddate)
    #print(lno(),df1)
    df_20=df1.tail(24).copy()
    try :    
        vol=df_20.iloc[-1]['vol']
        close=df_20.iloc[-1]['close']
        open=df_20.iloc[-1]['open']
        high=df_20.iloc[-1]['high']
        low=df_20.iloc[-1]['low']
        pre_close=df_20.iloc[-2]['close']
    except :
        print(lno(),stock_no,enddate,"Fail") 
        return 0
    #if flag=="otc"   :  
    #    print (lno(),stock_no)
    if type(close)==str :
        print (lno(),close)
    #print (lno(),i)
    if (len(df1)<120):
        return 0
    if vol*close< 3000000 :
        return 0

    #start=datetime.now() 
    #1. 漲停  close /pre_close >=1.08  
    #2.大紅k close -open / pre_close >8%
    #3.長下影  close -low / pre_close >=6.18%
    #4 半年新高 
    flag=0
    kpwr_m1_ratio=0.04
    if open >pre_close:
        kpwr_mode1= (close -pre_close)/pre_close
    else :
        kpwr_mode1= (close -open)/pre_close
        
    if kpwr_mode1>= kpwr_m1_ratio:
        #print("kpwr_mode1")
        df_120=df1.tail(120).copy()
        #print(lno(),df_120)
        df_120['kpwr_mode1']=df_120.apply(kpwr_mode1_calc, axis=1)
        top10=df_120.sort_values(by='kpwr_mode1',ascending=False).iloc[10]['kpwr_mode1']
        #print (lno(),top10)
        if kpwr_mode1>top10:
            flag=2
    
    if flag==0 and high!=low:
        base=close
        if open <close:
            base=open
        res=(base-low)/(high-low) 
        #print (lno(),res)
        if res >=0.618 and (base-low)/pre_close>=0.04 :
            flag=3
        
    if flag!=0 :        
        df_90=df1.tail(90).copy()
        ma_list = [89]
        for ma in ma_list:
            df_90.loc[:,'MA_' + str(ma)] = df_90['close'].rolling(window=ma,center=False).mean()
        #print (lno(),df_90.iloc[-1]['MA_89'],df_90.iloc[-2]['MA_89'])   
        if close > df_90.iloc[-1]['MA_89'] and df_90.iloc[-1]['MA_89']>=df_90.iloc[-2]['MA_89']:
            return flag    
    startdate = enddate-relativedelta(months=3)
    dist_df=get_tdcc_dist_df(stock_no,startdate,enddate)
    if dist_df.iloc[-1]['t_stocks']*dist_df.iloc[-1]['>400_percents']<=dist_df.iloc[-2]['t_stocks']*dist_df.iloc[-2]['>400_percents']:
        return 0
    if dist_df.iloc[-1]['t_stocks']*dist_df.iloc[-1]['>1000_percents']<=dist_df.iloc[-2]['t_stocks']*dist_df.iloc[-2]['>1000_percents']:
        return 0    
    df_20['b_pwr'] = df_20.apply(calc_bpwr, axis=1)
    df_20['s_pwr'] = df_20.apply(calc_spwr, axis=1)
    df_20['bs'] = df_20['s_pwr'].rolling(window=20,center=False).mean()/df_20['b_pwr'].rolling(window=20,center=False).mean() 
    
    bs=df_20.iloc[-1]['bs']
    if bs<=0.6 :
         
        flag=11
    #print (lno(),stock_no,bs)    
    #df_20['diff'] = df_20['close'] - df_20['close'].shift()
   
    max_120_5= max(df1['high'])
    high_20=max(df_20['high'])
    close= df_20.iloc[-1]['close']
    #print (lno(),close)
    if high_20==max_120_5 and close/high_20<=0.7 :
        flag=12
        
    if flag <10 and flag!=0:
        flag=13
   
    return flag

##practice mode 做多
def check_mode_p1(flag,stock_no,enddate):
    df1=get_stock_df_bydate_nums(stock_no,240,enddate)
    
    df_20=df1.tail(24).copy()
    try :
        vol=df_20.iloc[-1]['vol']
        close=df_20.iloc[-1]['close']
        open=df_20.iloc[-1]['open']
        high=df_20.iloc[-1]['high']
        low=df_20.iloc[-1]['low']
        pre_close=df_20.iloc[-2]['close']
    except :
        print (lno(),stock_no,enddate,'get data error')
        print (lno(),df_20.tail(3))
        
        return ''
    #if flag=="otc"   :  
    #    print (lno(),stock_no)
    if type(close)==str :
        print (lno(),close)
    #print (lno(),i)
    if (len(df1)<120):
        return ''
    if vol*close< 3000000 :
        return ''
    
    #start=datetime.now() 
    #1. 漲停  close /pre_close >=1.08  
    #2.大紅k close -open / pre_close >8%
    #3.長下影  close -low / pre_close >=6.18%
    #4 半年新高 
    flag=0
    kpwr_m1_ratio=0.035
    if open >pre_close:
        kpwr_mode1= (close -pre_close)/pre_close
    else :
        kpwr_mode1= (close -open)/pre_close
        
    if kpwr_mode1>= kpwr_m1_ratio:
        #print("kpwr_mode1")
        df_120=df1.tail(120).copy()
        #print (lno(),stock_no)
        df_120['kpwr_mode1']=df_120.apply(kpwr_mode1_calc, axis=1)
        top10=df_120.sort_values(by='kpwr_mode1',ascending=False).iloc[10]['kpwr_mode1']
        #print (lno(),top10)
        if kpwr_mode1>top10:
            flag=2
    
    if flag==0 and high!=low:
        base=close
        if open <close:
            base=open
        res=(base-low)/(high-low) 
        #print (lno(),res)
        if res >=0.618 and (base-low)/pre_close>=0.04 :
            flag=3
    if flag==0:
        df_20['diff_percent'] = df_20['diff'] / df_20['close'].shift()
        df_20['kline_mode1_percent'] = (df_20['close']-df_20['open']) / df_20['close'].shift()    
        df_20['kline_mode2_percent'] = (df_20['close']-df_20['low']) / df_20['close'].shift()
        if df_20.iloc[-1]['diff_percent']>=0.05 or df_20.iloc[-1]['kline_mode1_percent']>=0.05 or df_20.iloc[-1]['kline_mode2_percent']>=0.05:
            if df_20.iloc[-2]['diff_percent']>=0.05 or df_20.iloc[-2]['kline_mode1_percent']>=0.05 or df_20.iloc[-2]['kline_mode2_percent']>=0.05:
                if df_20.iloc[-3]['diff_percent']>=0.05 or df_20.iloc[-3]['kline_mode1_percent']>=0.05 or df_20.iloc[-3]['kline_mode2_percent']>=0.05:
                    flag=4    
    if flag!=0 :        
        ma_list = [5,21,89]
        for ma in ma_list:
            df1.loc[:,'MA_' + str(ma)] = df1['close'].rolling(window=ma,center=False).mean()
        df1['MA5_MA21']=df1['MA_5']-df1['MA_21'] 
        #print (lno(),df_90.iloc[-1]['MA_89'],df_90.iloc[-2]['MA_89'])   
        if close >= df1.iloc[-1]['MA_89'] and df1.iloc[-1]['MA_89']>= df1.iloc[-2]['MA_89']:
            return '{}:{}'.format(flag,get_240_kline_analy_mode(df1))
    return ''        

def check_mode_n1(flag,stock_no,enddate):
    df1=get_stock_df_bydate_nums(stock_no,240,enddate)
    
    df_20=df1.tail(24).copy()
    try :
        vol=df_20.iloc[-1]['vol']
        close=df_20.iloc[-1]['close']
        open=df_20.iloc[-1]['open']
        high=df_20.iloc[-1]['high']
        low=df_20.iloc[-1]['low']
        pre_close=df_20.iloc[-2]['close']
    except :
        print (lno(),stock_no,enddate,'get data error')
        print (lno(),df_20.tail(3))
        
        return ''
    #if flag=="otc"   :  
    #    print (lno(),stock_no)
    if type(close)==str :
        print (lno(),close)
    #print (lno(),i)
    if (len(df1)<120):
        return ''
    if vol*close< 3000000 :
        return ''
    
    #start=datetime.now() 
    #1. 漲停  close /pre_close >=1.08  
    #2.大紅k close -open / pre_close >8%
    #3.長下影  close -low / pre_close >=6.18%
    #4 半年新高 
    flag=0
    kpwr_m1_ratio=-0.035
    if open <pre_close:
        kpwr_mode1= (close -pre_close)/pre_close
    else :
        kpwr_mode1= (close -open)/pre_close
    #print(lno(),stock_no,kpwr_mode1)    
    if kpwr_mode1<= kpwr_m1_ratio:
        #print("kpwr_mode1")
        df_120=df1.tail(120).copy()
        #print (lno(),stock_no)
        df_120['kpwr_mode1']=df_120.apply(kpwr_mode1_calc, axis=1)
        top10=df_120.sort_values(by='kpwr_mode1',ascending=True).iloc[10]['kpwr_mode1']
        #print (lno(),top10)
        if kpwr_mode1<top10:
            flag=2
    
    if flag==0 and high!=low:
        base=close
        if open >close:
            base=open
            
        res=(high-base)/(high-low) 
        #print (lno(),res)
        if res >=0.618 and (high-base)/pre_close>=0.035 :
            flag=3
    if flag!=0 :        
        ma_list = [5,21,89]
        for ma in ma_list:
            df1.loc[:,'MA_' + str(ma)] = df1['close'].rolling(window=ma,center=False).mean()
        df1['MA5_MA21']=df1['MA_5']-df1['MA_21'] 
        #print (lno(),df_90.iloc[-1]['MA_89'],df_90.iloc[-2]['MA_89'])   
        if close <= df1.iloc[-1]['MA_89']:
            return '{}:{}'.format(flag,get_240_kline_analy_mode(df1))
    return ''            
    

def get_stock_df(stock_no):
    dstpath='data/%s.csv'%(stock_no)
    #print lno(),dstpath
    #df = pd.read_csv(dstpath,encoding = 'big5')
    df = pd.read_csv(dstpath,encoding = 'utf-8')
    df.dropna(axis=1,how='all',inplace=True)
    df.dropna(inplace=True)
    df.columns = ['date', 'vol', 'cash', 'open', 'high','low','close','diff','Tnumber','stock_name']
    df.drop('cash', axis=1, inplace = True)
    df.drop('Tnumber', axis=1, inplace = True)
    #tokline_type(df)
    for i in range (1,len(df)):
        if ( df.iloc[i]['open']=='--') or  ( df.iloc[i]['open']=='---') :
            df.ix[i,'open']=df.iloc[i-1]['open']
            df.ix[i,'high']=df.iloc[i-1]['high']
            df.ix[i,'low']=df.iloc[i-1]['low']
            df.ix[i,'close']=df.iloc[i-1]['close']
    df=df.replace('--',np.NaN)
    df=df.replace('---',np.NaN)
    df=df.dropna(how='any',axis=0)
    try :
        df['date']=[twdate2datetime64(x) for x in df['date'] ]
        
        df['open']=df['open'].astype('float64')
        df['high']=df['high'].astype('float64')
        df['low']=df['low'].astype('float64')
        df['close']=df['close'].astype('float64')
    except :
        print(lno(),df.tail(5))
    df=df.reset_index(drop=True)    
    #print(lno(),df.dtypes)
    
    return df      
        
def get_tdcc_dist_df(stock_no,startdate,enddate):
    tdcc_date_path='csv/tdcc_date.csv'
    date_df = pd.read_csv(tdcc_date_path,encoding = 'big5',header=None)
    date_df.dropna(axis=1,how='all',inplace=True)
    date_df.columns = ['date_str']
    #print (lno(),startdate,enddate)
    #print (lno(),date_df.iloc[105])
    date_df['date'] =  pd.to_datetime(date_df['date_str'], format='%Y%m%d')
    sample_df=get_df_bydate(date_df,startdate,enddate)
    #print (lno(),sample_df)
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
    #print (lno(),sample_df.head(5))
    for i in range(0, len(sample_df)):
        #print (lno(),i,sample_df.iloc[i]['date_str'])
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
                #print (lno(),j,sample_df.iloc[i]['date'],dist_df.iloc[j]['a2'],dist_df.iloc[j]['a4'])
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
def get_tdcc_dist_df_bydate_num(stock_no,enddate,num):
    tdcc_date_path='csv/tdcc_date.csv'
    date_df = pd.read_csv(tdcc_date_path,encoding = 'big5',header=None)
    date_df.dropna(axis=1,how='all',inplace=True)
    date_df.columns = ['date_str']
    #print (lno(),startdate,enddate)
    
    date_df['date'] =  pd.to_datetime(date_df['date_str'], format='%Y%m%d')
    sample_df=date_df[(date_df.loc[:,"date"] <= np.datetime64(enddate))] 
    sample_df=sample_df.head(num)
    #print (lno(),sample_df)
    outcols = ['date','t_stocks','t_persons','>400_stocks','>400_percents','>400_persons','>1000_stocks','>1000_percents','>1000_persons']       
    
    df = pd.DataFrame(pd.np.empty(( 1, len(outcols))) * pd.np.nan, columns = outcols)
    for i in range(0, len(sample_df)):
        #print (lno(),sample_df.iloc[i]['date_str'])
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
        dist_list.append(int(round(t_stocks/1000)))
        dist_list.append(int(t_persions))
        persion=0
        stocks=0
        percent=0.0
        # >=400 middle
        for j in range (11,15):
            #print (lno(),j,sample_df.iloc[i]['date'],dist_df.iloc[j]['a2'],dist_df.iloc[j]['a4'])
            persion+=dist_df.iloc[j]['a2']
            stocks+=dist_df.iloc[j]['a3']
            percent+=dist_df.iloc[j]['a4']  
        dist_list.append(int(round(stocks/1000)))
        dist_list.append(percent)
        dist_list.append(int(persion))
        dist_list.append(int(dist_df.iloc[14]['a3']/1000))
        dist_list.append(dist_df.iloc[14]['a4'])
        dist_list.append(int(dist_df.iloc[14]['a2']))
        df.loc[-1] = dist_list
        df.index = df.index + 1
    df.dropna(axis=0,how='all',inplace=True)    
    df.sort_values(by='date', ascending=True,inplace = True)
    df=df.reset_index(drop=True)
    #print (lno(),df)
    return df

    
   
    
def time64_date(x):
    ts = pd.to_datetime(str(x)) 
    d = ts.strftime('%Y%m%d')
    return d    
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
def get_df_bydate(df1,startdate,enddate):
    #print (lno(),df1['date'].dtype,type(enddate) )
    #print (lno(),df1['date'],enddate )
    df=df1[(df1.loc[:,"date"] <= np.datetime64(enddate)) & (df1.loc[:,"date"] >= np.datetime64(startdate))]
    return df
def kline_js_stock_mode_v1(name, df,prices_cols=None, render_path=None):
    '''
    @params:
    - name: str                      #图例名称
    - df:  pandas.DataFrame          #columns包含 prices_cols、‘volume’
    - prices_cols : list             #默认 [u'open', u'close', u'low', u'high']
    - render_path： str               #html file path to save
    '''
    if not prices_cols:
        prices_cols = [u'open', u'close', u'low', u'high']


    #grid1 =Grid(width=1280, height=800)
    grid1 =Grid(width=1000, height=800)
    #print (lno(),name)
    if debug==1:
        print(lno(),df.tail())
    kline_js,vol_js=stock_kline_vol_js_v1(name,df,800,600)
    grid1.add(kline_js,grid_top="3%", grid_bottom="45%",grid_left="3%",grid_right="3%")
    grid1.add(vol_js,  grid_top="55%", grid_bottom="32%",grid_left="3%",grid_right="3%")    

    
    dist_js,dist_js1=stock_dist_js(name,df)
    grid1.add(dist_js, grid_top="70%",grid_bottom="17%",grid_left="3%",grid_right="3%")        
    grid1.add(dist_js1, grid_top="83%",grid_bottom="3%",grid_left="3%",grid_right="3%")        


   
    
    #bs_line=stock_bs_js("test2",df)            
    #grid1.add(bs_line, grid_top="88%",grid_bottom="2%",grid_left="2%",grid_right="34%")
    #grid1.add(bs_line,  grid_top="55%", grid_bottom="32%",grid_left="3%",grid_right="3%")   
  
    #print (lno(),dist_df.tail(1))
    #print (lno(),render_path)
    if render_path:
        page = Page() 
        page.add(grid1)

        page.render(render_path)
        #make_a_snapshot('kline.html', render_path)
        #render_path
    return grid1  # .render('k-line.html')    
def stock_kline_vol_js_v1(name, df,width,height):
    if not 'date' in df.columns:
        DATE=pd.to_datetime(df.index).strftime('%y/%m/%d')
    else :
        DATE=df['date'].dt.strftime('%y/%m/%d')
    data = [[o,close,lowest,highest] for o,close,lowest,highest in zip(df['open'],df['close'],df['low'],df['high'])] 
    candlestick = Candlestick(name[0],title_top='3%')
    #is_xaxis_show=False
    print (lno(),len(df))
    lx= 240*100/len(df)
    candlestick.add(name[0], DATE, data, 
        is_legend_show=False,
        xaxis_pos='top', yaxis_pos='right',
        #yaxis_min=(min(df["low"])-(max(df["high"])-min(df["low"]))/4),
        mark_line=["min", "max"],mark_line_valuedim=['lowest', 'highest'],
        is_datazoom_show=True,datazoom_orient='horizontal', 
        datazoom_type='both',
        datazoom_xaxis_index=[0,1,2,3], datazoom_range=[0,lx ], 
        )
    
    vol_bar = Bar(name[3],title_top='11%')    
    vol_bar.add('vol',
            x_axis=DATE,
            y_axis=df['vol'].values.tolist(),
            #is_datazoom_show=True,
            #legend_top="55%",
            is_legend_show=False,
            #is_label_show=True,
            #label_text_size=8,
            is_xaxis_show=False,
            #is_stack=True,
            is_xaxislabel_align=True,
            yaxis_label_textsize =8,
            #yaxis_max=max(df["s_diff"])+ (max(df["s_diff"])-min(df["s_diff"]))/5,
            #yaxis_min=min(df["s_diff"])- (max(df["s_diff"])-min(df["s_diff"]))/5
            )    
    """        
    vol_line = Line("")
    vol_line.add(
        "vol", DATE, df['vol'].values.tolist(),
        is_legend_show=False,
        is_fill=True,area_color='#808080', is_xaxis_show=False, is_yaxis_show=False,
        line_opacity=0.2,
        area_opacity=0.4,
        symbol=None,
        #yaxis_max=5*max(df["vol"])
        )
    """        
    Line_draw = True
    ma89_line = Line()
    ma89_line.add('MA_89', DATE, df['MA_89'].values.tolist()) 
    overlap = Overlap()
    overlap.add(candlestick)  # overlap kline and ma
    overlap.add(ma89_line)  
    return overlap,vol_bar 
def stock_dist_js_new1(name, df):
    
    dist_bar = Bar()
    dist_diffs =df[df['t_persons']!=np.NaN]

    if not 'date' in dist_diffs.columns:
        DATE=pd.to_datetime(dist_diffs.index).strftime('%y/%m/%d')
    else :
        DATE=dist_diffs['date'].dt.strftime('%y/%m/%d')
    dist_bar.add('t_persons',
            x_axis=DATE,
            y_axis=dist_diffs['t_persons'].values.tolist(),
            #is_datazoom_show=True,
            legend_top="70%",
            is_legend_show=False,
            is_xaxis_show=False,
            #is_stack=True,
            is_xaxislabel_align=True,
            yaxis_label_textsize =8,
            #yaxis_max=max(df["s_diff"])+ (max(df["s_diff"])-min(df["s_diff"]))/5,
            #yaxis_min=min(df["s_diff"])- (max(df["s_diff"])-min(df["s_diff"]))/5
            )
    dist_bar1 = Bar()        
    dist_bar1.add('>400_percents',
            x_axis=DATE,
            y_axis=dist_diffs['>400_percents'].values.tolist(),
            #is_datazoom_show=True,
            legend_top="70%",
            is_legend_show=False,
            is_xaxis_show=False,
            #is_stack=True,
            yaxis_label_textsize =8,
            is_xaxislabel_align=True,
            #yaxis_max=max(df["s_diff"])+ (max(df["s_diff"])-min(df["s_diff"]))/5,
            #yaxis_min=min(df["s_diff"])- (max(df["s_diff"])-min(df["s_diff"]))/5
            )
    #print (lno(),dist_diffs['b_diff'].values.tolist())  
    #print (lno(),dist_diffs)  
    #dist_diffs['b_diff']=dist_diffs['b_diff'].shift(-1)
    #print (lno(),dist_diffs['b_diff'].values.tolist())
              
    dist_bar1.add('>1000_percents',
            x_axis=DATE,
            y_axis=dist_diffs['>1000_percents'].values.tolist(),
            #is_datazoom_show=True,
            legend_top="70%",
            is_legend_show=False,
            is_xaxis_show=False,
            #is_stack=True,
            yaxis_label_textsize =8,
            is_xaxislabel_align=True,
            #yaxis_max=max(df["s_diff"])+ (max(df["s_diff"])-min(df["s_diff"]))/5,
            #yaxis_min=min(df["s_diff"])- (max(df["s_diff"])-min(df["s_diff"]))/5
            )        
    
    return dist_bar,dist_bar1  
    
def stock_dist_js(name, df,pos1='70%',pos2='83%',):
    
    dist_bar = Bar(name[2],title_top=pos1)
    dist_diffs =df[df['t_persons_diff']!=np.NaN]

    if not 'date' in dist_diffs.columns:
        DATE=pd.to_datetime(dist_diffs.index).strftime('%y/%m/%d')
    else :
        DATE=dist_diffs['date'].dt.strftime('%y/%m/%d')
    dist_bar.add('t_persons_diff',
            x_axis=DATE,
            y_axis=dist_diffs['t_persons_diff'].values.tolist(),
            #is_datazoom_show=True,
            legend_top=pos1,
            is_legend_show=True,
            is_label_show=True,
            label_text_size=8,
            is_xaxis_show=False,
            #is_stack=True,
            is_xaxislabel_align=True,
            yaxis_label_textsize =8,
            #yaxis_max=max(df["s_diff"])+ (max(df["s_diff"])-min(df["s_diff"]))/5,
            #yaxis_min=min(df["s_diff"])- (max(df["s_diff"])-min(df["s_diff"]))/5
            )
    
    #dist_bar1 = Bar(name[1],title_top=pos2)        
    dist_bar1 = Bar(name[1],title_top='7%')
    dist_bar1.add('>400',
            x_axis=DATE,
            y_axis=dist_diffs['m_diff'].values.tolist() ,
            #is_datazoom_show=True,
            legend_top=pos2,
            is_legend_show=True,
            #is_label_show=True,
            #label_text_size=8,
            is_xaxis_show=False,
            #is_stack=True,
            yaxis_label_textsize =8,
            is_xaxislabel_align=True,
            #yaxis_max=max(df["s_diff"])+ (max(df["s_diff"])-min(df["s_diff"]))/5,
            #yaxis_min=min(df["s_diff"])- (max(df["s_diff"])-min(df["s_diff"]))/5
            )
    #print (lno(),dist_diffs['b_diff'].values.tolist())  
    #print (lno(),dist_diffs)  
    dist_diffs['b_diff']=dist_diffs['b_diff'].shift(-1)
    #print (lno(),dist_diffs['b_diff'].values.tolist())
              
    dist_bar1.add('>1000',
            x_axis=DATE,
            y_axis= dist_diffs['b_diff'].values.tolist() ,
            #is_datazoom_show=True,
            legend_top=pos2,
            #legend_pos='right',
            is_legend_show=True,
            is_label_show=True,
            label_text_size=8,
            is_xaxis_show=False,
            #is_stack=True,
            yaxis_label_textsize =8,
            is_xaxislabel_align=True,
            #yaxis_max=max(df["s_diff"])+ (max(df["s_diff"])-min(df["s_diff"]))/5,
            #yaxis_min=min(df["s_diff"])- (max(df["s_diff"])-min(df["s_diff"]))/5
            )        
    
    return dist_bar,dist_bar1  
def stock_bs_js(name, df):
    if not 'date' in df.columns:
        DATE=pd.to_datetime(df.index).strftime('%y/%m/%d')
    else :
        DATE=df['date'].dt.strftime('%y/%m/%d')
    bs_line = Line()
    bs_line.add("bs", DATE, df['bs'],line_color=['rgba(0,0,255,255)'],
                #is_xaxis_show=False,
                legend_pos='left',
                is_legend_show=False,
                legend_top="65%", is_stack=True,is_add_yaxis=True,is_xaxislabel_align=True,yaxis_pos='left')
    return bs_line   
def np64toDatetime(np64):
    ts = (np64 - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
    return datetime.utcfromtimestamp(ts)

def calc_b_stocks(row):
    tt=(row['t_stocks']*row['>1000_percents'])/100
    try :
        tt=int(tt)
        return tt
    except :    
        print(lno(),row['>1000_percents'])
        return tt
def generate_stock_kline_html_v1(stock_no,title,begindate,html_filen,practice=0):
    result=[]
    sr_list=[]
    
    month=0
    #startdate=begindate- relativedelta(months=18)
    #nowdatetime = begindate+relativedelta(months=1)
    #nowdatetime = begindate
    df_in = pd.DataFrame()
    df1 = pd.DataFrame()
    list_ = []
    
    #df0=stock_comm.get_stock_df(stock_no)
    #print (lno(),df0.tail(1))
    #stock_df=get_df_bydate(df0,startdate,nowdatetime)
    df0=get_stock_df(stock_no)
    if debug==2:
        print (lno(),df0)
    len_df=len(df0)

    pos=df0[(df0.loc[:,"date"] <= np.datetime64(begindate))].tail(1).index.values[0]
    
    if pos>=239:
        start_pos=pos-239
    else :
        start_pos=0
        
    if len_df>=pos+21:
        end_pos=pos+20
    else:
        end_pos=len_df
    #print (lno(),type(begindate))
    #print (lno(),type(df0.iloc[start_pos]['date'].values[0]))
    #print(lno(),df0[1:5])
    stock_df=df0[start_pos:end_pos]
    stock_df=stock_df.reset_index(drop=True)
    if debug==1:
        print(lno(),begindate,len(stock_df),stock_df)
    try:
        startdate= np64toDatetime(df0.iloc[start_pos]['date'])   
        nowdatetime= np64toDatetime(df0.iloc[end_pos-1]['date'])   
    except:    
        print (lno(),stock_df)
        print (lno(),begindate,start_pos,end_pos)
        
        return
    #print(lno(),pos,len_df)
    #return 
    #stock_df=get_stock_df_bydate_nums(stock_no,300,nowdatetime)
    #print (lno(),stock_df.tail(1),)
    #print (lno(),nowdatetime)
    #startdate=stock_df.at[0,'date']
    dist_df=get_tdcc_dist_df(stock_no,startdate,nowdatetime)
    #print (lno(),dist_df.tail(4))
    #print (lno(),stock_df.tail())
    df=stock_df.sort_values(by='date', ascending=True)
    df=df.replace('--',np.NaN)
    df=df.replace('---',np.NaN)
    df=df.dropna(how='any',axis=0)
    df['b_pwr'] = df.apply(calc_bpwr, axis=1)
    df['s_pwr'] = df.apply(calc_spwr, axis=1)
    #print (lno(),df)
    ma_list = [89]
    for ma in ma_list:
        df['MA_' + str(ma)] = df['close'].rolling(window=ma,center=False).mean()
    """
    ma_list = [5, 20]
    for ma in ma_list:
        df['MA_' + str(ma)] = df['close'].rolling(window=ma,center=False).mean()
    #print lno(),df[['date','close','MA_5']]       
    df['MA5_MA20']=df.apply(calc_ma5_20,axis=1)
    """

    df['bs'] = df['s_pwr'].rolling(window=20,center=False).mean()/df['b_pwr'].rolling(window=20,center=False).mean() 
    sample_df=df[df.loc[:,"date"] <= np.datetime64(nowdatetime)]
    #new_df=sample_df.tail(250)
    #dist_df=dist_df.tail(48)
    #print (lno(),dist_df)
    dist_df['t_persons_diff']=dist_df['t_persons'] - dist_df['t_persons'].shift()
    dist_df['m_diff']=dist_df['>400_stocks'] - dist_df['>400_stocks'].shift()
    dist_df['b_stocks']=dist_df.apply(calc_b_stocks, axis=1)
    dist_df['b_diff'] = dist_df['b_stocks'] - dist_df['b_stocks'].shift()
    #dist_df['b_diff']=(dist_df['t_stocks']*dist_df['>1000_percents'] -dist_df['t_stocks'].shift()*dist_df['>1000_percents'].shift())/100
    
    

    new_df=sample_df.set_index('date').join(dist_df.set_index('date'))
    new_df['t_persons'].fillna(method='ffill', inplace=True)
    new_df['>1000_percents'].fillna(method='ffill', inplace=True)
    new_df['>400_percents'].fillna(method='ffill', inplace=True)
    #print (lno(),new_df.tail(1))
   
    fin_df=new_df.tail(300)
    #print (lno(),fin_df.tail(1))
    """  
    for i in range(0, len(fin_df)):
        print (lno(),fin_df.iloc[i]['high'],fin_df.iloc[i]['low'])  
    """    
    dst_folder='data/html/{}'.format(stock_no)
    check_dst_folder(dst_folder)
    #html_filen='data/html/{0}/{1}.html'.format(stock_no,begindate.strftime("%Y-%m-%d"))
    #print(lno(),title)
    #pos=len(dist_df)
    """
    test_df=dist_df.dropna()
    test_df=test_df[test_df.loc[:,"date"] <= np.datetime64(begindate) ]
    w4_b=sum(int(i) for i in  test_df.tail(4)['b_diff'].values.tolist())
    w4_m=sum(int(i) for i in  test_df.tail(4)['m_diff'].values.tolist())
    bb=test_df.tail(1)['b_diff'].values.tolist()[0]
    mm=test_df.tail(1)['m_diff'].values.tolist()[0]
    pp=test_df.tail(1)['t_persons_diff'].values.tolist()[0]
    try:
        bb=int(bb)
        mm=int(mm)
    except:
        print (lno(),bb,mm,test_df)
        bb=0
        mm=0
    """
    dist_df=get_tdcc_dist_df_bydate_num(stock_no,nowdatetime,5)
    #print (lno(),dist_df.iloc[-1])
    #print (lno(),dist_df.iloc[0])
    pp=dist_df.iloc[-1]['t_persons']-dist_df.iloc[-2]['t_persons']
    mm=dist_df.iloc[-1]['>400_stocks']-dist_df.iloc[-2]['>400_stocks']
    bb=dist_df.iloc[-1]['>1000_stocks']-dist_df.iloc[-2]['>1000_stocks']
    w4_p=dist_df.iloc[-1]['t_persons']-dist_df.iloc[0]['t_persons']
    w4_m=dist_df.iloc[-1]['>400_stocks']-dist_df.iloc[0]['>400_stocks']
    w4_b=dist_df.iloc[-1]['>1000_stocks']-dist_df.iloc[0]['>1000_stocks']
    otc_df=get_otc_exchange_data(nowdatetime)
    otc_list=otc_df['stock_id'].values.tolist()
    fflag=''
    if stock_no in otc_list:
        print(lno(),"otc")
        fflag='otc'
        rev_month,rev_df=get_revenue_by_stockid(stock_no,nowdatetime,'otc')
        eps_list=get_eps_by_stockid(stock_no,nowdatetime,'otc')
        print(lno(),eps_list)
    else:    
        print(lno(),"twse")
        fflag='tse'
        rev_month,rev_df=get_revenue_by_stockid(stock_no,nowdatetime,'tse')
        eps_list=get_eps_by_stockid(stock_no,nowdatetime,'tse')
        print(lno(),eps_list)
    m_rev=np.nan
    acc_rev=np.nan
    if len(rev_df)!=0:
        m_rev=rev_df.iloc[0]['去年同月增減(%)']
        acc_rev=rev_df.iloc[0]['前期比較增減(%)']

    eps=np.nan
    prev_eps=np.nan    
    if eps_list!=[]:
        eps=eps_list[2]
        prev_eps=eps_list[3]    
    #hh=fin_df.tail(1)['high'].values.tolist()[0]
    #ll=fin_df.tail(1)['low'].values.tolist()[0]
    stock_3big_df=stock_big3.get_stock_3big(stock_no,nowdatetime,5,fflag)
    #print(lno(),stock_3big_df)
    
    #print(lno(),stock_3big_df.loc[:,'外資'].values.tolist())
    print(lno(),stock_3big_df.loc[:,'外資'].values.tolist(),stock_3big_df.loc[:,'投信'].values.tolist(),stock_3big_df.loc[:,'自營商'].values.tolist())
    title1='大戶動向:周1000={:d}, 周400={:d}, 月1000={:d}, 月400={:d}'.format(int(bb),int(mm),int(w4_b),int(w4_m))
    title2='股票總人數:周人數增加={:d} ,月人數增加={:d} '.format(int(pp),int(w4_p))
    title0='{}:月營收成長={}, 累計營收成長={}, 本年累計eps={}, 去年同期eps={}'.format(title,m_rev,acc_rev,eps,prev_eps)    
    title3='外資:{}投信:{}自營商:{}'.format(stock_3big_df.loc[:,'外資'].values.tolist(),stock_3big_df.loc[:,'投信'].values.tolist(),stock_3big_df.loc[:,'自營商'].values.tolist())
    print(lno(),title3)
    title_new=[]
    title_new.append(title0)
    title_new.append(title1)
    title_new.append(title2)
    title_new.append(title3)
    grid=kline_js_stock_mode_v1(title_new,fin_df,list(fin_df),html_filen)
    print(lno(),title_new,time64_date(begindate))
    if practice>=1 :
        if  out_path=='' :
            outpath=dataday.strftime("%Y%m")
        else :
            outpath=out_path
        if practice==1:
            outf='practice/{}/{}_practice.md'.format(outpath,time64_date(dataday))
        else:
            outf='practice/{}/{}_practice_n1.md'.format(outpath,time64_date(dataday))
        #outstr='{} {}'.format(title_new,time64_date(begindate))
        print(title_new,time64_date(begindate), file=open(outf, "a", encoding = 'utf-8'))
        today= datetime.today().date()
        enddate=datetime(year=today.year,month=today.month,day=today.day,)
        df0=get_stock_df(stock_no)
        #print(lno(),df0)
        fin_df0=get_df_bydate(df0,begindate,enddate)
        #print(lno(),fin_df0)
        if len(fin_df0)>2:
            if practice==1:
                if fin_df0.iloc[1]['open']<=fin_df0.iloc[0]['high']:
                    print('未高開', file=open(outf, "a", encoding = 'utf-8'))
                print('{} 買進:{} 防守:{}'.format(time64_date(fin_df0.iloc[1]['date']),fin_df0.iloc[1]['open'],fin_df0.iloc[1]['low']), file=open(outf, "a", encoding = 'utf-8'))
                buy=fin_df0.iloc[1]['open']
                
                for j in range(2, len(fin_df0)):
                    if fin_df0.iloc[j]['close']>=fin_df0.iloc[j-1]['low']:
                        print('{} 收盤:{} > 防守:{}'.format(time64_date(fin_df0.iloc[j]['date']),fin_df0.iloc[j]['close'],fin_df0.iloc[j-1]['low']), file=open(outf, "a", encoding = 'utf-8'))
                    else :
                        print('{} 收盤:{} < 防守:{}'.format(time64_date(fin_df0.iloc[j]['date']),fin_df0.iloc[j]['close'],fin_df0.iloc[j-1]['low']), file=open(outf, "a", encoding = 'utf-8'))
                        if j+1<=  len(fin_df0)-1 :
                            try :
                                sell=fin_df0.iloc[j+1]['open']
                                print('{} 賣出:{} 獲利:{:.2f}%'.format(time64_date(fin_df0.iloc[j+1]['date']),sell,(sell-buy)/buy*100  ), file=open(outf, "a", encoding = 'utf-8') )
                                break
                            except:
                                print (lno(),j,len(fin_df0))  
            elif practice==2:
                if fin_df0.iloc[1]['open']>fin_df0.iloc[0]['low']:
                    print('未低開', file=open(outf, "a", encoding = 'utf-8'))
                print('{} 放空:{} 防守:{}'.format(time64_date(fin_df0.iloc[1]['date']),fin_df0.iloc[1]['open'],fin_df0.iloc[1]['high']), file=open(outf, "a", encoding = 'utf-8'))
                sell=fin_df0.iloc[1]['open']
                
                for j in range(2, len(fin_df0)):
                    if fin_df0.iloc[j]['close']<=fin_df0.iloc[j-1]['high']:
                        print('{} 收盤:{} < 防守:{}'.format(time64_date(fin_df0.iloc[j]['date']),fin_df0.iloc[j]['close'],fin_df0.iloc[j-1]['high']), file=open(outf, "a", encoding = 'utf-8'))
                    else :
                        print('{} 收盤:{} > 防守:{}'.format(time64_date(fin_df0.iloc[j]['date']),fin_df0.iloc[j]['close'],fin_df0.iloc[j-1]['high']), file=open(outf, "a", encoding = 'utf-8'))
                        if j+1<=  len(fin_df0)-1 :
                            try :
                                buy=fin_df0.iloc[j+1]['open']
                                print('{} 回補:{} 獲利:{:.2f}%'.format(time64_date(fin_df0.iloc[j+1]['date']),buy,(sell-buy)/buy*100  ), file=open(outf, "a", encoding = 'utf-8') )
                                break
                            except:
                                print (lno(),j,len(fin_df0))  
    return grid
def get_df_bydate(df1,startdate,enddate):
    #print (lno(),df1['date'].dtype,type(enddate) )
    #print (lno(),df1['date'],enddate )
    df=df1[(df1.loc[:,"date"] <= np.datetime64(enddate)) & (df1.loc[:,"date"] >= np.datetime64(startdate))]
    return df
def tokline_type(df):
    df['date']=[twdate2datetime64(x) for x in df['date'] ]
    df['open']=[float(x.strip().replace(',', '')) for x in df['open'] ]
    df['high']=[float(x.strip().replace(',', '')) for x in df['high'] ]
    df['low']=[float(x.strip().replace(',', '')) for x in df['low'] ]
    df['close']=[float(x.strip().replace(',', '')) for x in df['close'] ]
    df['vol']=[float(x.strip().replace(',', '')) for x in df['vol'] ]
    df['diff']=[float(x) for x in df['diff'] ]    
def get_tse_month_df(nowdatetime):
    dstpath='%s/%d%02d'%(TSE_KLINE_PATH,int(nowdatetime.year), int(nowdatetime.month))
    df =pd.read_csv(dstpath,encoding = 'big5',skiprows=1)
    df.dropna(axis=1,how='all',inplace=True)
    df.dropna(inplace=True)
    df.columns = ['date', 'open', 'high', 'low', 'close']
    df['date']=[twdate2datetime64(x) for x in df['date'] ]
    df['open']=[float(x.strip().replace(',', '')) for x in df['open'] ]
    df['high']=[float(x.strip().replace(',', '')) for x in df['high'] ]
    df['low']=[float(x.strip().replace(',', '')) for x in df['low'] ]
    df['close']=[float(x.strip().replace(',', '')) for x in df['close'] ]
    return df
def get_tse_kline_df(dataday):
    result=[]
    sr_list=[]
    nowdatetime = dataday
    month=0
    startdate=dataday- relativedelta(months=15)
   
    df_in = pd.DataFrame()
    df1 = pd.DataFrame()
    list_ = []
    while   nowdatetime>=startdate :
        #print lno(),nowdatetime
        df1= get_tse_month_df(nowdatetime)
        #print lno(),df1
        list_.append(df1)
        month=month+1
        nowdatetime = dataday - relativedelta(months=month)
    df_in=pd.concat(list_)
    df=df_in.sort_values(by='date', ascending=True)
    df=df.tail(240)
    #print df.loc[:, ['date','MA5_MA20','open','high','low','close']]
    ma_list = [5, 21,89]
    for ma in ma_list:
        df['MA_' + str(ma)] = df['close'].rolling(window=ma,center=False).mean()
    #print (lno(),df[['date','close','MA_5']])
    df['MA5_MA21']=df['MA_5']-df['MA_21']  
    #df['bs'] = df['s_pwr'].rolling(window=20,center=False).mean()/df['b_pwr'].rolling(window=20,center=False).mean() 
    return df
def get_240_kline_analy_mode(df):
    highest= max(df['high'])
    lowest= min(df['low'])
    close=df.iloc[-1]['close']
    
    high_pos=highest-((highest-lowest)/3)
    low_pos=lowest+(highest-lowest)/3
    if close >high_pos:
        pos_mode='高位階'
    elif close <low_pos:     
        pos_mode='低位階'
    else :
        pos_mode='中位階'
    #print (lno(),close,highest,lowest,high_pos,low_pos)
    trend=df.iloc[-1]['MA_89']-df.iloc[-2]['MA_89']    
    if trend >0 and close>df.iloc[-1]['MA_89'] :
        trend_mode='多頭趨勢'
    elif trend <0 and close<df.iloc[-1]['MA_89'] :    
        trend_mode='空頭趨勢'
    else :
        trend_mode='盤整'    
    trend=df.iloc[-1]['MA5_MA21']-df.iloc[-2]['MA5_MA21']    
    trend1=df.iloc[-2]['MA5_MA21']-df.iloc[-3]['MA5_MA21']    
    if df.iloc[-1]['MA5_MA21']>0 :
        if trend>0:
            life_mode='老'
        else:
            life_mode='病'
    else :
        if trend>0:
            life_mode='生'
        else:
            life_mode='死'  
    out_str='{},{},{}'.format(pos_mode,trend_mode,life_mode)        
    return out_str        
def get_market_mode(dataday):
    """
    ##位階判斷:一年內high -low 三等份
    ##趨勢判斷:(89ma up close>89ma)多 (89ma down close<89ma)
    ##周期判斷: MA5_MA21>0  1.MA5_MA21>=MA5_MA21_p  =老
                            2.MA5_MA21<MA5_MA21_p   =病
                MA5_MA21<=0 1.MA5_MA21>=MA5_MA21_p  =生
                            2.MA5_MA21<MA5_MA21_p   =死                                
    """
    print (lno(),dataday)
    df=get_tse_kline_df(dataday)
    #print (lno(),df)
    mode=get_240_kline_analy_mode(df)
    #print (lno(),mode)
    tse_pwr_list=powerinx.get_tse_power_inx(dataday,'up')
    #print (lno(),tse_pwr_list)    
    otc_pwr_list=powerinx.get_otc_power_inx(dataday,'up')
    print (lno(),mode)
    print (lno(),tse_pwr_list,otc_pwr_list)
    return mode,tse_pwr_list,otc_pwr_list        
def Time64ToDate(x):
    ts = pd.to_datetime(str(x)) 
    d = ts.strftime('%Y%m%d')
    return d                
def practice_mode1(dataday):
    if  out_path=='' :
        outpath=dataday.strftime("%Y%m")
    else :
        outpath=out_path
    print (lno(),out_path)
        
    dstpath='practice/save'
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath)   
    dstpath='practice/{}'.format(outpath)
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath) 
    outf='practice/{}/{}_practice.md'.format(outpath,time64_date(dataday))
    try:
        os.remove(outf)
    except OSError:
        pass    
    practice=1
    try:
        mode,tse_pwr_list,otc_pwr_list=get_market_mode(dataday)
    except:
        print (lno(),'fail')
        return
    print('大盤',mode, file=open(outf, "a", encoding = 'utf-8'))
    print('上市:',tse_pwr_list, file=open(outf, "a", encoding = 'utf-8'))
    print('上櫃:',otc_pwr_list, file=open(outf, "a", encoding = 'utf-8'))
    twse_df=get_TWSE_df()
    #print (lno(),len(twse_df))
    otc_df=get_OTC_df()
    #print (lno(),len(otc_df))
    filen='practice/save/{}.csv'.format(Time64ToDate(dataday))
    if os.path.exists(filen):
        print (lno())
        need_save=0
        filen='practice/save/{}.csv'.format(Time64ToDate(dataday))
        d = pd.read_csv(filen,encoding = 'utf-8',header=None,names=['no','name','d1','d2'],dtype= {'no':str, 'name': str,'d1':str, 'd2': str})
    else :
        d=pd.concat([twse_df, otc_df], axis=0, join='inner')
        d.reset_index(inplace=True)
        print (lno(),d)
        need_save=1
        
    page1 = Page() 
    page2 = Page() 

    page1_cnt=0
    page2_cnt=0  
    #d=  otc_df
    save_stock_list=[]
    for j in range(0, len(d)):
        i=d.at[j,'no']
        #title='{}:{}'.format(i,d.at[j,'name']) 
        #print (lno(),i)
        if i[0:2]=='00' or i[0:2]=='28':
            #print (lno(),i[0:2]    )
            continue
        #if i!='3642':
        #    continue
        #start=datetime.now()
        if '上市' in d.at[j,'d2']:
            flag='twse'
        else :    
            flag='otc'
            
        #mode=check_mode(i,enddate)
        mode=check_mode_p1(flag,i,dataday)
        title='{}:{}:{}'.format(i,d.at[j,'name'],mode)
        #print (lno(),title)
        ## 3日紅k ,20日 買力>賣力
        if mode!='':
            """
            if flag=='twse':
                if d.at[j,'d1'] in tse_pwr_list:
                    title='{}:主流'.format(title)
            else :
                if d.at[j,'d1'] in otc_pwr_list:
                    title='{}:主流'.format(title)
            """        
            grid=generate_stock_kline_html_v1(i,title,dataday,None,practice)
            if flag=='twse':
                #print (lno(),tse_pwr_list)
                #print (lno(),d.at[j,'d1'])
                page1.add(grid)  
                page1_cnt+=1
            else :    
                page2.add(grid)  
                page2_cnt+=1
            save_stock_list.append([d.at[j,'no'],d.at[j,'name'],d.at[j,'d1'],d.at[j,'d2']])   
            #print (lno(),save_stock_list)    
        #if mode >0 :
        #    print (lno(),title)    
            
    if (page1_cnt>0):
        html_filen='practice/{}/{}_twse_mode1.html'.format(outpath,dataday.strftime("%Y%m%d"))  
        page1.render(html_filen) 
    if (page2_cnt>0):
        html_filen='practice/{}/{}_otc_mode1.html'.format(outpath,dataday.strftime("%Y%m%d"))  
        page2.render(html_filen)
    if (need_save==1):
        labels = ['no','name','d1','d2']
        df_save = pd.DataFrame.from_records(save_stock_list, columns=labels)
        filen='practice/save/{}.csv'.format(Time64ToDate(dataday))
        df_save.to_csv(filen,encoding='utf-8', index=False,header=0) 

        
def practice_moden1(dataday):
    if  out_path=='' :
        outpath=dataday.strftime("%Y%m")
    else :
        outpath=out_path
    outf='practice/{}/{}_practice_n1.md'.format(outpath,time64_date(dataday))
    try:
        os.remove(outf)
    except OSError:
        print (lno(),'fail')
        pass
        
    dstpath='practice/save'
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath)   
    dstpath='practice/{}'.format(outpath)
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath) 
    practice=2
    try:
        mode,tse_pwr_list,otc_pwr_list=get_market_mode(dataday)
        print('大盤',mode, file=open(outf, "a", encoding = 'utf-8'))
        print('上市:',tse_pwr_list, file=open(outf, "a", encoding = 'utf-8'))
        print('上櫃:',otc_pwr_list, file=open(outf, "a", encoding = 'utf-8'))
    except:
        print (lno(),'fail')
        #return
    
   
    d=get_over50_list()
    #print (lno(),len(twse_df))
  
    #print (lno(),len(otc_df))
    
    need_save=0
        
    page1 = Page() 

    page1_cnt=0
    #d=  otc_df
    save_stock_list=[]
    for j in d:
        i=str(j)
        #title='{}:{}'.format(i,d.at[j,'name']) 
        #print (lno(),i)
        if i[0:2]=='00' or i[0:2]=='28':
            #print (lno(),i[0:2]    )
            continue
        mode=check_mode_n1('ttt',i,dataday)
        title='{}:{}'.format(i,mode)
        #print (lno(),title)

        if mode!='':
            grid=generate_stock_kline_html_v1(i,title,dataday,None,practice)
            #print (lno(),d.at[j,'d1'])
            page1.add(grid)  
            page1_cnt+=1
            
    if (page1_cnt>0):
        html_filen='practice/{}/{}_mode2.html'.format(outpath,dataday.strftime("%Y%m%d"))  
        page1.render(html_filen) 

    
def check_long_mode1(flag,stock_no,enddate):
    df1=comm.get_stock_df_bydate_nums(stock_no,240,enddate)
    #print(lno(),df1)
    df_20=df1.tail(24).copy()
    try :    
        vol=df_20.iloc[-1]['vol']
        close=df_20.iloc[-1]['close']
        open=df_20.iloc[-1]['open']
        high=df_20.iloc[-1]['high']
        low=df_20.iloc[-1]['low']
        pre_close=df_20.iloc[-2]['close']
    except :
        print(lno(),stock_no,enddate,"Fail") 
        return 0,0
    #if flag=="otc"   :  
    #    print (lno(),stock_no)
    if type(close)==str :
        print (lno(),close)
    #print (lno(),i)
    if (len(df1)<120):
        return 0,0
    if vol*close< 3000000 :
        return 0,0

    #start=datetime.now() 
    #1. 漲停  close /pre_close >=1.08  
    #2.大紅k close -open / pre_close >8%
    #3.長下影  close -low / pre_close >=6.18%
    #4 半年新高 
    flag=0
    kpwr_m1_ratio=0.04
    if open >pre_close:
        kpwr_mode1= (close -pre_close)/pre_close
    else :
        kpwr_mode1= (close -open)/pre_close
        
    if kpwr_mode1>= kpwr_m1_ratio:
        #print("kpwr_mode1")
        df_120=df1.tail(120).copy()
        #print(lno(),df_120)
        df_120['kpwr_mode1']=df_120.apply(kpwr_mode1_calc, axis=1)
        top10=df_120.sort_values(by='kpwr_mode1',ascending=False).iloc[10]['kpwr_mode1']
        #print (lno(),top10)
        if kpwr_mode1>=top10:
            flag=2
    
    if flag==0 and high!=low:
        base=close
        if open <close:
            base=open
        res=(base-low)/(high-low) 
        #print (lno(),res)
        if res >=0.618 and (base-low)/pre_close>=0.04 :
            flag=3
        
    if flag!=0 :        
        df_90=df1.tail(90).copy()
        ma_list = [89]
        for ma in ma_list:
            df_90.loc[:,'MA_' + str(ma)] = df_90['close'].rolling(window=ma,center=False).mean()
        #print (lno(),df_90.iloc[-1]['MA_89'],df_90.iloc[-2]['MA_89'])   
        #if close > df_90.iloc[-1]['MA_89'] and df_90.iloc[-1]['MA_89']>=df_90.iloc[-2]['MA_89']:
        if df_90.iloc[-1]['MA_89']>=df_90.iloc[-2]['MA_89'] :
            startdate = enddate-relativedelta(months=3)
            dist_df=get_tdcc_dist_df(stock_no,startdate,enddate)
            #print(lno(),stock_no,dist_df.tail())
            stable_add=dist_df.iloc[-1]['>400_stocks']-dist_df.iloc[-2]['>400_stocks']
            total_vol= df_90[(df_90.loc[:,"date"] <= dist_df.iloc[-1]['date']) & (df_90.loc[:,"date"] > dist_df.iloc[-2]['date'])]['vol'].sum()/1000
            #week_df['vol'].sum()
            print(lno(),stock_no,stable_add,total_vol)
            return flag,stable_add/total_vol    
    startdate = enddate-relativedelta(months=3)
    dist_df=get_tdcc_dist_df(stock_no,startdate,enddate)
    if dist_df.iloc[-1]['>400_stocks']<=dist_df.iloc[-2]['>400_stocks']:
        return 0,0
    if dist_df.iloc[-1]['t_stocks']*dist_df.iloc[-1]['>1000_percents']<=dist_df.iloc[-2]['t_stocks']*dist_df.iloc[-2]['>1000_percents']:
        return 0,0    
    df_20['b_pwr'] = df_20.apply(calc_bpwr, axis=1)
    df_20['s_pwr'] = df_20.apply(calc_spwr, axis=1)
    df_20['bs'] = df_20['s_pwr'].rolling(window=20,center=False).mean()/df_20['b_pwr'].rolling(window=20,center=False).mean() 
    flag=0
    bs=df_20.iloc[-1]['bs']
    if bs<=0.6 :
         
        flag=11
    #print (lno(),stock_no,bs)    
    #df_20['diff'] = df_20['close'] - df_20['close'].shift()
   
    max_120_5= max(df1['high'])
    high_20=max(df_20['high'])
    close= df_20.iloc[-1]['close']
    #print (lno(),close)
    if high_20==max_120_5 and close/high_20<=0.7 :
        flag=12
        
    if flag <10 and flag!=0:
        flag=13
   
    if flag!=0:
        stable_add=dist_df.iloc[-1]['>400_stocks']-dist_df.iloc[-2]['>400_stocks']
        total_vol= df_20[(df_20.loc[:,"date"] <= dist_df.iloc[-1]['date']) & (df_20.loc[:,"date"] > dist_df.iloc[-2]['date'])]['vol'].sum()/1000
        return flag,stable_add/total_vol    
    return 0,0

def generate_stock_kline_html_all(d,market,mode,enddate,practice):
    page1 = Page() 
    page1_cnt=0
    for j in range(0, len(d)):
        i=d.at[j,'stock_id']
        title='{}:{}:{}'.format(i,d.at[j,'stock_name'],mode)
        grid=generate_stock_kline_html_v1(i,title,enddate,None,practice)
        page1.add(grid)  
        page1_cnt+=1
    
    html_filen='out/stock_{}_{}_{}.html'.format(market,mode,enddate.strftime("%Y-%m-%d"))  
    if (page1_cnt>0):
        page1.render(html_filen)     
##for 股job        
def generate_stock_kline_html_all_mode2(enddate,practice=0,mode='股job'):
    a=[['6192','巨路'],
    ['8480','泰昇-KY'],
    ['8114','振樺電'],
    ['6670','復盛應用'],
    ['4912','聯德控股-KY'],
    ['4163','鐿鈦'],
    ['4438','廣越'],
    ['1593','祺驊'],
    ['8499','鼎炫-KY'],
    ['5283','禾聯碩'],
    ['3491','昇達科'],
    ['6504','南六'],
    ['8466','美吉吉-KY'],
    ['5312','寶島科'],
    ['6561','是方'],
    ['2929','淘帝-KY'],
    ]
    d = pd.DataFrame(a)
    d.columns = ['stock_id','stock_name']
    print(lno(),d)

    page1 = Page() 
    page1_cnt=0
    for j in range(0, len(d)):
        i=d.at[j,'stock_id']
        title='{}:{}:{}'.format(i,d.at[j,'stock_name'],mode)
        grid=generate_stock_kline_html_v1(i,title,enddate,None,practice)
        page1.add(grid)  
        page1_cnt+=1
    
    html_filen='out/stock_{}_{}.html'.format(mode,enddate.strftime("%Y-%m-%d"))  
    if (page1_cnt>0):
        page1.render(html_filen)
def get_revenue_by_stockid(stock_id,enddate,market):
    month=0
    while   month<=3 :
        nowdatetime = enddate - relativedelta(months=month) 
        revenue_csv='data/revenue/%s_%d-%02d.csv'% (market,nowdatetime.year-1911,nowdatetime.month )
        if os.path.exists(revenue_csv):
            df_s = pd.read_csv(revenue_csv,encoding = 'utf-8',dtype= {'公司代號':str})
            df_s.dropna(axis=1,how='all',inplace=True)
            df_s.dropna(inplace=True)
            df=df_s.loc[df_s['公司代號'] == stock_id]
            if len(df)==0:
                revenue_csv='data/revenue/%s_%d-%02d_1.csv'% (market,nowdatetime.year-1911,nowdatetime.month )
                if os.path.exists(revenue_csv):
                    df_s = pd.read_csv(revenue_csv,encoding = 'utf-8',dtype= {'公司代號':str})
                    df_s.dropna(axis=1,how='all',inplace=True)
                    df_s.dropna(inplace=True)
                    df=df_s.loc[df_s['公司代號'] == stock_id] 
            print (lno(),df)
            return nowdatetime.month,df
        
        month=month+1
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
  
def generate_stock_html_mode2(enddate,practice=0,mode='股job',in_df=[],debug=1):
    SaveDirectory = os.getcwd()
    if mode=='股job':
        #"""
        a=[
        ['6192','巨路'],
        ['8480','泰昇-KY'],
        ['8114','振樺電'],
        ['6670','復盛應用'],
        #['4912','聯德控股-KY'],
        ['4163','鐿鈦'],
        ['4438','廣越'],
        ['2308','台達電'],
        ['8499','鼎炫-KY'],
        ['5283','禾聯碩'],
        ['3491','昇達科'],
        ['6504','南六'],
        ['8466','美吉吉-KY'],
        #['5312','寶島科'],
        #['6561','是方'],
        ]
        #"""
        #a=[['6192','巨路']]
        d = pd.DataFrame(a)
        #d.columns = ['公司代號','公司名稱','建議買價','賣出價1','賣出價2','預計eps']
        d.columns = ['公司代號','公司名稱']
    elif mode=='rev':
        out_file='csv/rev_good.csv'
        df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype= {'公司代號':str})
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        d=df_s[['公司代號','公司名稱']].copy()
        print(lno(),d)
    else:
        d=in_df[0][['stock_id','stock_name']].copy()
        #a=[['2308','台達電']]
        #d = pd.DataFrame(a)
        d.columns=['公司代號','公司名稱']
    otc_df=comm.get_otc_exchange_data(enddate,ver=1)
    #print(lno(),otc_df)
    otc_list=otc_df['stock_id'].values.tolist()
    check_dst_folder('out/%d%02d%02d'%(enddate.year,enddate.month,enddate.day))
    #print(lno(),otc_list)
    
    if debug:
        print(lno(),d)
    d["現價"] = np.nan    
    d["日周K"] = ""
    d["周K"] = ""
    d["日K"] = ""
    d["周人數"] = 0
    d["周籌碼"] = 0.0
    d["周股權分佈"] = ""
    d["月人數"] = 0
    d["月籌碼"] = 0.0
    d["月股權分佈"] = ""
    d["三率"] = ""
    d["月營收成長"] = ""
    d["累計營收成長"] = ""
    d["累計eps"] = ""
    d["去年同期eps"] = ""
    d['三大法人']=""
    stk=comm.stock_data()
    #d['特殊信號']= np.nan
    for j in range(0, len(d)):
        stock_no=d.at[j,'公司代號']
        if debug:
            print(lno(),stock_no)
        #print(lno(),stock_no)
        #df=get_stock_df_bydate_nums(stock_no,90,enddate)
        sdate=enddate-relativedelta(months=16)
        edate=enddate+relativedelta(days=1)
        df=stk.get_df_by_startdate_enddate(stock_no,sdate,edate)
        #df['vol']=df['vol'].astype(float)
        d.at[j,'現價']=df.iloc[-1]['close']
        
        check_dst_folder('out/%d%02d%02d/kline'%(enddate.year,enddate.month,enddate.day))    
        filen='out/%d%02d%02d/kline/%s_day.png'%(enddate.year,enddate.month,enddate.day,stock_no)
        title='%s %s 現價:%.2f'%(d.at[j,'公司代號'],d.at[j,'公司名稱'],d.at[j,'現價'])
        kline.generate_stock_day_kline(stock_no,enddate,filen,title,df_in=df.copy())
        d.at[j,'日K']= 'kline/%s_day.png'%(stock_no)
        filen='out/%d%02d%02d/kline/%s_week.png'%(enddate.year,enddate.month,enddate.day,stock_no)
        kline.generate_stock_week_kline(stock_no,enddate,filen,df_in=df.copy())
        d.at[j,'周K']= 'kline/%s_week.png'%(stock_no)
        
        filen='%s/out/%d%02d%02d/kline/%s_day_week.png'%(SaveDirectory,enddate.year,enddate.month,enddate.day,stock_no)
        kline.generate_stock_kline_pic(stock_no,enddate,filen,df_in=df.copy())
        d.at[j,'日周K']= 'file://%s'%(filen)
        
        #d.at[j,'中線']= life_mode   
       
        check_dst_folder('out/%d%02d%02d/stock_big3'%(enddate.year,enddate.month,enddate.day))  
        filen_0='out/%d%02d%02d/stock_big3/%s_3big.png'%(enddate.year,enddate.month,enddate.day,stock_no)
        stock_big3.generate_stock_3big_pic(stock_no,enddate,filen_0)
        d.at[j,'三大法人']='stock_big3/%s_3big.png'%(stock_no)
        
        list=tdcc_dist.get_tdcc_dist_all_df_bydate_num(stock_no,enddate,5)
        check_dst_folder('out/%d%02d%02d/tdcc'%(enddate.year,enddate.month,enddate.day))    
        filen_1='out/%d%02d%02d/tdcc/%s_week.png'%(enddate.year,enddate.month,enddate.day,stock_no)
        persion_add,percent=tdcc_dist.tdcc_dist_plot(stock_no,enddate,list[0],list[1],filen_1)
        d.at[j,'周人數']=persion_add
        d.at[j,'周籌碼']=percent
        d.at[j,'周股權分佈']='tdcc/%s_week.png'%(stock_no)
        filen_2='out/%d%02d%02d/tdcc/%s_month.png'%(enddate.year,enddate.month,enddate.day,stock_no)
        persion_add,percent=tdcc_dist.tdcc_dist_plot(stock_no,enddate,list[0],list[4],filen_2)
        d.at[j,'月人數']=persion_add
        d.at[j,'月籌碼']=percent
        d.at[j,'月股權分佈']='tdcc/%s_month.png'%(stock_no)
        #print (lno(),dist_df.iloc[-1])
        #print (lno(),dist_df.iloc[0])
        arr = [filen_0, filen_1, filen_2]
        toImage = Image.new('RGBA',(1400,2400))
        for i in range(len(arr)):
            fromImge = Image.open(arr[i])
            mew_im = fromImge.resize((1400, 800), Image.ANTIALIAS) 
            # loc = ((i % 2) * 200, (int(i/2) * 200))
            loc = (0,800*i)
            print(loc)
            toImage.paste(mew_im, loc)
        filen_3='out/%d%02d%02d/tdcc/%s_fin.png'%(enddate.year,enddate.month,enddate.day,stock_no)
        toImage.save(filen_3)
        d.at[j,'籌碼(三大/周/月)']='tdcc/%s_fin.png'%(stock_no)
        check_dst_folder('out/%d%02d%02d/eps'%(enddate.year,enddate.month,enddate.day))  
        filen='out/%d%02d%02d/eps/%s_3ratio.png'%(enddate.year,enddate.month,enddate.day,stock_no)
        eps.gen_3_rate_pic(stock_no,enddate,filen)
        d.at[j,'三率']='eps/%s_3ratio.png'%(stock_no)
        
       
        """
        d.at[j,'周人數']=dist_df.iloc[-1]['t_persons']-dist_df.iloc[-2]['t_persons']
        d.at[j,'周400']=dist_df.iloc[-1]['>400_stocks']-dist_df.iloc[-2]['>400_stocks']
        d.at[j,'周1000']=dist_df.iloc[-1]['>1000_stocks']-dist_df.iloc[-2]['>1000_stocks']
        d.at[j,'月人數']=dist_df.iloc[-1]['t_persons']-dist_df.iloc[0]['t_persons']
        d.at[j,'月400']=dist_df.iloc[-1]['>400_stocks']-dist_df.iloc[0]['>400_stocks']
        d.at[j,'月1000']=dist_df.iloc[-1]['>1000_stocks']-dist_df.iloc[0]['>1000_stocks']
        """
        if stock_no in otc_list:
            print(lno(),"otc")
            rev_month,rev_df=revenue.get_revenue_by_stockid(stock_no,enddate)
            eps_list=get_eps_by_stockid(stock_no,enddate,'otc')
            print(lno(),eps_list)
        else:    
            print(lno(),"twse",stock_no,enddate)
            rev_month,rev_df=revenue.get_revenue_by_stockid(stock_no,enddate)
            eps_list=get_eps_by_stockid(stock_no,enddate,'tse')
            print(lno(),eps_list)
        if len(rev_df)!=0:
            d.at[j,'累計營收成長']=str(rev_df.iloc[0]['前期比較增減(%)'])
            d.at[j,'月營收成長']=str(rev_df.iloc[0]['去年同月增減(%)'])+"("+str(rev_month)+")"
            #d.at[j,'累計營收成長']=str(rev_df.iloc[0]['前期比較增減(%)'])+"("+str(rev_month)+")"
        if eps_list!=[]:
            d.at[j,'累計eps']=str(eps_list[2])+"("+str(eps_list[1])+")"
            d.at[j,'去年同期eps']=str(eps_list[3])
    d=d.round({'月籌碼': 1, '周籌碼': 1})
    d['周股權分佈'] = d['周股權分佈'].apply(lambda x: '<img src="{}" style="max-height:124px;"/>'.format(x) if x else '') 
    d['月股權分佈'] = d['月股權分佈'].apply(lambda x: '<img src="{}" style="max-height:124px;"/>'.format(x) if x else '') 
    d['籌碼(三大/周/月)'] = d['籌碼(三大/周/月)'].apply(lambda x: '<img src="{}" style="max-height:360px;"/>'.format(x) if x else '') 
    d['周K'] = d['周K'].apply(lambda x: '<img src="{}" style="max-height:360px;"/>'.format(x) if x else '') 
    d['日K'] = d['日K'].apply(lambda x: '<img src="{}" style="max-height:360px;"/>'.format(x) if x else '') 
    d['日周K'] = d['日周K'].apply(lambda x: '<img src="{}" style="max-height:360px;"/>'.format(x) if x else '') 
    d['三大法人'] = d['三大法人'].apply(lambda x: '<img src="{}" style="max-height:124px;"/>'.format(x) if x else '') 
    d['三率'] = d['三率'].apply(lambda x: '<img src="{}" style="max-height:124px;"/>'.format(x) if x else '') 
    d['基本面'] ='營收'+'<br>'+d['月營收成長']+'<br>'+'累計營收'+'<br>'+d['累計營收成長']+'<br>'+'季EPS'+'<br>'+d['累計eps']+'<br>'+'去年同期'+'<br>'+d['去年同期eps']
    #d['基本面'] ='Revenue'+'<br>'+d['月營收成長']+'<br>'+'Total Revenue'+'<br>'+d['累計營收成長']+'<br>'+'Season EPS'+'<br>'+d['累計eps']+'<br>'+'Last Year'+'<br>'+d['去年同期eps']
    #d['基本面'] = d['基本面'].apply(lambda x: '<p style="max-width:120px;"> {}""</p>"'.format(x) if x else '') 
    #d.style.applymap(max_width_limit, subset=['基本面'])
    #str_mon='%d月營收成長'%(rev_month) 
    #str_eps='%d季累計eps'%(eps_list[1])     
    
    #d.rename(columns={'月營收成長':str_mon}, inplace=True)
    #d.rename(columns={'累計eps':str_eps}, inplace=True)
    #print(lno(),str_mon,d.columns)
    d.sort_values(by='周籌碼', ascending=False,inplace = True)
    d=d.reset_index(drop=True)
    d1=d[['日K','周K','籌碼(三大/周/月)','三率','基本面']]
    #d1.columns=['day candlestick chart','week candlestick chart','(Three major/tdcc week/tdcc month)','Gross Profit/earning/net income','Revenue/EPS']
    d1.columns=['日K','周K','籌碼(法人/周/月股權)','毛利/營益/淨利率','營收/EPS']

    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)
    filen='out/%d%02d%02d/%s.html'%(enddate.year,enddate.month,enddate.day,mode)
    d1.to_html(filen,escape=False,index=False,sparsify=True,border=2,index_names=False)
    pd.set_option('display.max_colwidth', old_width) 
    PdfFilename='out/%d%02d%02d/%s.pdf'%(enddate.year,enddate.month,enddate.day,mode)
    options = {
            'page-size': 'B3',  
            'margin-top': '0.75in',  
            'margin-right': '0.75in',  
            'margin-bottom': '0.75in',  
            'margin-left': '0.75in',  
            'encoding': "BIG5",
            'no-outline': None
            } 
            
    #pdf.from_file(filen, PdfFilename,options=options)

    """
    app = QApplication(sys.argv)
    doc = QTextDocument()
    location = filen
    html = open(location).read()
    doc.setHtml(html)
    printer = QPrinter()
    printer.setOutputFileName("foo.pdf")
    printer.setOutputFormat(QPrinter.PdfFormat)
    printer.setPageSize(QPrinter.A4)
    printer.setPageMargins(15, 15, 15, 15, QPrinter.Millimeter)
    doc.print_(printer)
    print("done!")
    """        
def generate_tdcc_good_html(enddate):
    out_file='csv/tdcc_good.csv'
    df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype= {'stock_id':str})
    df_s.dropna(axis=1,how='all',inplace=True)
    df_s.dropna(inplace=True)
    print(lno(),df_s)
    d=df_s[['stock_id','stock_name']].copy()
    generate_stock_html_mode2(dataday,mode='tdcc',in_df=[d])
if __name__ == '__main__':
    #global out_path
    sns.set()
    mpl.rcParams['font.family'] = 'sans-serif'
    mpl.rcParams['font.sans-serif'] = 'SimHei'
    if len(sys.argv)>1 and sys.argv[1]=='1' :
        dataday=datetime.strptime(sys.argv[2],'%Y%m%d')
        practice_mode1(dataday)
    elif len(sys.argv)>1 and sys.argv[1]=='2' :
        dataday=datetime.strptime(sys.argv[2],'%Y%m%d')
        practice_moden1(dataday)

    elif len(sys.argv)>2 and sys.argv[2]=='b' :     
        practice=0
        dataday=datetime.strptime(sys.argv[1],'%Y%m%d')
        enddate=dataday
    elif len(sys.argv)>2 and sys.argv[1]=='j' :     
        practice=0
        dataday=datetime.strptime(sys.argv[2],'%Y%m%d')
        generate_stock_kline_html_all_mode2(dataday)
        generate_stock_html_mode2(dataday)
    elif len(sys.argv)>2 and sys.argv[1]=='job' :     
        practice=0
        dataday=datetime.strptime(sys.argv[2],'%Y%m%d')
        #generate_stock_kline_html_all_mode2(dataday)
        generate_stock_html_mode2(dataday)
    elif len(sys.argv)>2 and sys.argv[1]=='rev' :     
        practice=0
        dataday=datetime.strptime(sys.argv[2],'%Y%m%d')
        #generate_stock_kline_html_all_mode2(dataday)
        generate_stock_html_mode2(dataday,mode='rev')    
    elif len(sys.argv)>2 and sys.argv[1]=='tdcc' :     
        practice=0
        dataday=datetime.strptime(sys.argv[2],'%Y%m%d')
        generate_tdcc_good_html(dataday)    
    elif len(sys.argv)>2 and sys.argv[1]=='-t' :     
        practice=0
        dataday=datetime.strptime(sys.argv[2],'%Y%m%d')
        generate_stock_html_mode2(dataday,mode='xxx')        
    else :
        practice=0
        dataday=datetime.strptime(sys.argv[1],'%Y%m%d')
        if len(sys.argv)>2 and sys.argv[2]=='4' :
            outf='out/practice_{}.csv'.format(time64_date(dataday))
            try:
                os.remove(outf)
            except OSError:
                pass
            practice=1
        
        enddate=dataday

        #d=get_TWSE_df()
        d=comm.get_tse_exchange_data(dataday)
        d['kline_stable']=np.nan
        d['bs_stable']=np.nan
        t=0

        for j in range(0, len(d)):
            i=d.at[j,'stock_id']

            try:
                tt=int(i)
            except:
                continue
            if i[0:2]=='00' or i[0:2]=='01' or len(i)>=5:
                #print (lno(),i[0:2]    )
                continue
            if i=='0000':
                continue
            diff=d.at[j,'diff']
            try:
                diff=float(d.at[j,'diff'])
                if diff<=0:
                    continue    
            except:
                print(lno(),d.iloc[j])
                continue
            #mode=check_mode(i,enddate)
            flag='twse'
            mode,stable_ratio=check_long_mode1(flag,i,enddate)
            
            ## bs power mode
            if mode>10:
                d.at[j,'bs_stable']= stable_ratio
            ##k line mode
            elif mode>0:
                d.at[j,'kline_stable']= stable_ratio
               
        d_kline=d[d['kline_stable'].notnull()].copy()
        d_kline.sort_values(by='kline_stable',ascending=False,inplace=True)
        d_kline=d_kline.reset_index(drop=True)
        print(lno(),d_kline)
        #20190908 test marked
        #generate_stock_kline_html_all(d_kline,'twse','kline',enddate,practice)
        generate_stock_html_mode2(enddate,mode='twse_kline',in_df=[d_kline])
        d_bs=d[d['bs_stable'].notnull()].copy()
        d_bs.sort_values(by='bs_stable',ascending=False,inplace=True)
        d_bs=d_bs.reset_index(drop=True)
        print(lno(),d_bs)
        #20190908 test marked
        #generate_stock_kline_html_all(d_bs,'twse','bs_power',enddate,practice)
        generate_stock_html_mode2(enddate,mode='twse_bs_power',in_df=[d_bs])

        d=comm.get_otc_exchange_data(dataday)
        d['kline_stable']=np.nan
        d['bs_stable']=np.nan
        flag='otc'
        for j in range(0, len(d)):
            i=d.at[j,'stock_id']
            if len(i)>=5:
                continue
            if i=='0000':
                continue    
            diff=d.at[j,'diff']
            try:
                diff=float(d.at[j,'diff'])
                if diff<=0:
                    continue    
            except:
                print(lno(),d.iloc[j])
                continue
            mode,stable_ratio=check_long_mode1(flag,i,enddate)
            ## bs power mode
            if mode>10:
                d.at[j,'bs_stable']= stable_ratio
            ##k line mode
            elif mode>0:
                d.at[j,'kline_stable']= stable_ratio
        d_kline=d[d['kline_stable'].notnull()].copy()
        d_kline.sort_values(by='kline_stable',ascending=False,inplace=True)
        d_kline=d_kline.reset_index(drop=True)
        print(lno(),d_kline)
        #20190908 test marked
        #generate_stock_kline_html_all(d_kline,'otc','kline',enddate,practice)
        generate_stock_html_mode2(enddate,mode='otc_kline',in_df=[d_kline])
        d_bs=d[d['bs_stable'].notnull()].copy()
        d_bs.sort_values(by='bs_stable',ascending=False,inplace=True)
        d_bs=d_bs.reset_index(drop=True)
        print(lno(),d_bs)
        #20190908 test marked
        #generate_stock_kline_html_all(d_bs,'otc','bs_power',enddate,practice)
        if len(d_bs)!=0:
            generate_stock_html_mode2(enddate,mode='otc_bs_power',in_df=[d_bs])
        #generate_stock_kline_html_all_mode2(enddate)
       
#"E:\Program Files\Oracle\VirtualBox\VBoxManage.exe""  internalcommands sethduuid f:\vbox\xxx.vdi        
#setup-x86_64.exe -q -s "http://cygwin.mirror.constant.com" -R "F:\Cygwin" -P "cygunsrv,Base,vim,vim-common,openssh,openssl,wget,unzip,rsync" -C "F:\Cygwin_packages" -l "F:\Cygwin_packages"
##ssh -fCNR 1556:localhost:22 root@36.230.26.96 
##ssh -p 1556 root@localhost