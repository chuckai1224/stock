# -*- coding: utf-8 -*-
#from __future__ import unicode_literals
import csv
import os
import time
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
from mpl_finance import candlestick_ohlc
import matplotlib.pyplot as plt
#import matplotlib.ticker as ticker
from matplotlib import ticker as mticker
from matplotlib.dates import num2date, date2num
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter,MonthLocator, WeekdayLocator,DayLocator, WEDNESDAY
import matplotlib.dates
import pandas
import numpy as np
import pandas as pd
import requests
import inspect
import tdcc_dist
import eps
from inspect import currentframe, getframeinfo

import seaborn as sns
import matplotlib as mpl
import stock_comm as comm
from matplotlib import ticker
import mpl_finance
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)
TWIIPATH='csv/twii'
TSE_KLINE_PATH='csv/tse_kline'
def check_dst_folder(dstpath):
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath)  
def test1(df):
    dates=mdates.date2num(df['date'])
    print (lno(),dates)
    kurse_o=df['open'].tolist()
    kurse_h=df['high'].tolist()
    kurse_l=df['low'].tolist()
    kurse_c=df['close'].tolist()
    quotes = [tuple([dates[i],
                     kurse_o[i],
                     kurse_h[i],
                     kurse_l[i],
                     kurse_c[i]]) for i in range(len(dates))] #_1

    #fig, ax = plt.subplots()
    fig = plt.figure(facecolor='#07000d',figsize=(12,9))

    
    ax = plt.subplot2grid((6,4), (1,0), rowspan=4, colspan=4, facecolor='#07000d') 
    candlestick_ohlc(ax, quotes, width=0.6, colorup='#ff1717', colordown='#53c156')

   

    # ---------------------------------------------------------

    fig.autofmt_xdate()
    fig.tight_layout()
    ax.grid(True, color='w')

    ax.xaxis.set_major_locator(mticker.MaxNLocator(10))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.yaxis.label.set_color("b")
    ax.spines['bottom'].set_color("#5998ff")
    ax.spines['top'].set_color("#5998ff")
    ax.spines['left'].set_color("#5998ff")
    ax.spines['right'].set_color("#5998ff")
    ax.tick_params(axis='y', colors='b')
    plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
    ax.tick_params(axis='x', colors='b')
    """
    volumeMin = 0
    ax1v = ax.twinx()
    ax1v.fill_between(daysreshape.DateTime.values[-SP:],volumeMin, days.Volume.values[-SP:], facecolor='#00ffe8', alpha=.4)
    ax1v.axes.yaxis.set_ticklabels([])
    ax1v.grid(False)
    ###Edit this to 3, so it's a bit larger
    ax1v.set_ylim(0, 3*days.Volume.values.max())
    ax1v.spines['bottom'].set_color("#5998ff")
    ax1v.spines['top'].set_color("#5998ff")
    ax1v.spines['left'].set_color("#5998ff")
    ax1v.spines['right'].set_color("#5998ff")
    ax1v.tick_params(axis='x', colors='w')
    ax1v.tick_params(axis='y', colors='w')
    """
    plt.savefig('Test.png')

    #plt.show()
def download_file(url, filename):
    ''' Downloads file from the url and save it as filename '''
    # check if file already exists
    print('Downloading File')
    response = requests.get(url)
    # Check if the response is ok (200)
    if response.status_code == 200:
        # Open file and write the content
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in response:
                file.write(chunk)
def down_tse_kline(startdate,enddate):
    result=[]
    sr_list=[]
    check_dst_folder(TSE_KLINE_PATH)
    nowdatetime = enddate
    month=0
    while   nowdatetime>=startdate :
        print (lno(),nowdatetime)
        dstpath='%s/%d%02d'%(TSE_KLINE_PATH,int(nowdatetime.year), int(nowdatetime.month))
        url_get0='http://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=csv&date=%d%02d%02d'%(int(nowdatetime.year),int(nowdatetime.month),int(nowdatetime.day))
        download_file(url_get0,dstpath)
        time.sleep(3)
        month=month+1
        nowdatetime = enddate - relativedelta(months=month)
    return []
def parse_tse_kline(startdate,enddate):
    dst_folder=TSE_KLINE_PATH
    out_file='{}/tse_kline_data_all.csv'.format(dst_folder)
    
    check_dst_folder(dst_folder)

    nowdatetime = enddate
    month=0
    while   nowdatetime>=startdate :
        print (lno(),nowdatetime)
        filename='%s/%d%02d'%(TSE_KLINE_PATH,int(nowdatetime.year), int(nowdatetime.month))
        if os.path.exists(filename):
            df = pd.read_csv(filename,encoding = 'big5',skiprows=1)
            df.dropna(axis=1,how='all',inplace=True)
            df.dropna(inplace=True)
            print(lno(),df)

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

        month=month+1
        nowdatetime = enddate - relativedelta(months=month)
    return []    
def get_tse_df_from_csv(startdate,enddate):
    dstpath='csv/tse.csv'
    df = pd.read_csv(dstpath,encoding = 'big5',skiprows=1)
    df.dropna(axis=1,how='all',inplace=True)
    df.dropna(inplace=True)
    df.columns = ['date','time','open','high','low','close','vol','money']
    df.drop('time', axis=1, inplace = True)
    df.drop('money', axis=1, inplace = True)
    ## date str type YYYY/MM/DD
    #df['date'] = df['date'].astype('datetime64[ns]')
    df['date'] =  pd.to_datetime(df['date'], format='%Y%m%d')
    #print (lno(),df.tail(3), np.datetime64(enddate),df.loc[1,"date"])
    if enddate==None :
      df=df[ df.loc[:,"date"] <= np.datetime64(startdate)]
      df=df.tail(360)
    else :
      df=df[ (df.loc[:,"date"] <= np.datetime64(enddate)) & (df.loc[:,"date"] >= np.datetime64(startdate))]
    print (lno(),len(df),df.tail(3))
    return df
def resample(in_df,period,cnt):
    # https://pandas-docs.github.io/pandas-docs-travis/timeseries.html#offset-aliases
    # 周 W、月 M、季度 Q、10天 10D、2周 2W
    #    period = 'W'
    df=in_df.set_index('date')
    print (lno(),df.tail(2))
    #df.set_index('date', inplace=True)
    weekly_df = df.resample(period).last()
    weekly_df['open'] = df['open'].resample(period).first()
    weekly_df['high'] = df['high'].resample(period).max()
    weekly_df['low'] = df['low'].resample(period).min()
    weekly_df['close'] = df['close'].resample(period).last()
    weekly_df['vol'] = df['vol'].resample(period).sum()
    #weekly_df['amount'] = df['amount'].resample(period, how='sum')
    # 去除空的数据（没有交易的周）
    weekly_df = weekly_df.dropna(how='any',axis=0)
    weekly_df.reset_index(inplace=True)
    return weekly_df.tail(cnt).copy()
def generate_candle_vol(df,ax):
    dates=mdates.date2num(df['date'])
    kurse_o=df['open'].tolist()
    kurse_h=df['high'].tolist()
    kurse_l=df['low'].tolist()
    kurse_c=df['close'].tolist()
    quotes = [tuple([dates[i],
                     kurse_o[i],
                     kurse_h[i],
                     kurse_l[i],
                     kurse_c[i]]) for i in range(len(dates))] #_1
    SP = len(df.date.values[0:])
    candlestick_ohlc(ax, quotes, width=0.6, colorup='#ff1717', colordown='#53c156')
    ax.grid(True, color='#808a87')
    #ax1.xaxis.set_major_locator(mticker.MaxNLocator(10))
    #wednesdays = WeekdayLocator(WEDNESDAY)
    #ax1.xaxis.set_major_locator(wednesdays)
    
    
    ax.yaxis.label.set_color("b")
    ax.spines['bottom'].set_color("#5998ff")
    ax.spines['top'].set_color("#5998ff")
    ax.spines['left'].set_color("#5998ff")
    ax.spines['right'].set_color("#5998ff")
    ax.tick_params(axis='y', colors='w')
    #plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
    #ax.tick_params(axis='x', colors='w',rotation=270 )
    ax.tick_params(axis='x', colors='w')
    #plt.ylabel('Stock price and Volume')
    volumeMin = 0
    ax1v = ax.twinx()
    ax1v.fill_between(df.date.values[-SP:],volumeMin, df.vol.values[-SP:], facecolor='#00ffe8', alpha=.4)
    ax1v.axes.yaxis.set_ticklabels([])
    ax1v.grid(False)
    ###Edit this to 3, so it's a bit larger
    ax1v.set_ylim(0, 3*df.vol.values.max())
    ax1v.spines['bottom'].set_color("#5998ff")
    ax1v.spines['top'].set_color("#5998ff")
    ax1v.spines['left'].set_color("#5998ff")
    ax1v.spines['right'].set_color("#5998ff")
    ax1v.tick_params(axis='x', colors='b')
    ax1v.tick_params(axis='y', colors='b')


def generate_kline_pic_org(in_df):
    week_df=resample(in_df,'W',72)
    #mon_df=resample(df,'M',24)
    df=in_df.tail(180)
    daysreshape = df.drop('vol', axis=1)
    # convert the datetime64 column in the dataframe to 'float days'
    daysreshape['date']=mdates.date2num(daysreshape['date'])
    # clean day data for candle view 
    daysreshape = daysreshape.reindex(columns=['date','open','high','low','close'])
    SP = len(daysreshape.date.values[0:])
    fig = plt.figure(facecolor='#07000d',figsize=(18,6))
    ax1 = plt.subplot2grid((1,10), (0,0), rowspan=1, colspan=7, facecolor='#07000d')
    ax2 = plt.subplot2grid((1,10), (0,7), rowspan=1, colspan=3, facecolor='#07000d')
    #ax3 = plt.subplot2grid((2,3), (1,2), rowspan=1, colspan=1, facecolor='#07000d')
    generate_candle_vol(df,ax1)
   
    generate_candle_vol(week_df,ax2)
    #generate_candle_vol(mon_df,ax3)
    #months = mdates.MonthLocator()
    #ax1.xaxis.set_major_locator(months)
    ax1.xaxis.set_major_locator(mticker.MaxNLocator(18))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%y%m%d'))
    ax1.yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
    ax2.xaxis.set_major_locator(mticker.MaxNLocator(18))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m'))
    #ax3.xaxis.set_major_locator(months)
    #ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m'))
    plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
    ax1.yaxis.tick_right()
    ax1.yaxis.set_ticks_position('both')
    ax2.yaxis.tick_right()
    ax2.yaxis.set_ticks_position('both')
    #ax3.yaxis.tick_right()
    #ax3.yaxis.set_ticks_position('both')
    #plt.ylabel('Stock price and Volume')
    """
    candlestick_ohlc(ax1, daysreshape.values[-SP:], width=.6, colorup='#ff1717', colordown='#53c156')
    ax1.grid(True, color='w')
    #ax1.xaxis.set_major_locator(mticker.MaxNLocator(10))
    #wednesdays = WeekdayLocator(WEDNESDAY)
    #ax1.xaxis.set_major_locator(wednesdays)
    months = mdates.MonthLocator()
    ax1.xaxis.set_major_locator(months)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    
    ax1.yaxis.label.set_color("b")
    ax1.spines['bottom'].set_color("#5998ff")
    ax1.spines['top'].set_color("#5998ff")
    ax1.spines['left'].set_color("#5998ff")
    ax1.spines['right'].set_color("#5998ff")
    ax1.tick_params(axis='y', colors='b')
    plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
    ax1.tick_params(axis='x', colors='b',rotation=270 )
    plt.ylabel('Stock price and Volume')
    volumeMin = 0
    ax1v = ax1.twinx()
    ax1v.fill_between(daysreshape.date.values[-SP:],volumeMin, df.vol.values[-SP:], facecolor='#00ffe8', alpha=.4)
    ax1v.axes.yaxis.set_ticklabels([])
    ax1v.grid(False)
    ###Edit this to 3, so it's a bit larger
    ax1v.set_ylim(0, 3*df.vol.values.max())
    ax1v.spines['bottom'].set_color("#5998ff")
    ax1v.spines['top'].set_color("#5998ff")
    ax1v.spines['left'].set_color("#5998ff")
    ax1v.spines['right'].set_color("#5998ff")
    ax1v.tick_params(axis='x', colors='b')
    ax1v.tick_params(axis='y', colors='b')
    """
    plt.savefig('Test.png')
    plt.show()  
    print (lno(),SP,daysreshape)
    #test1(df)
def generate_kline_pic(in_df):
    #week_df=resample(in_df,'W',72)
    #mon_df=resample(df,'M',24)
    df=in_df.tail(180)
    daysreshape = df.drop('vol', axis=1)
    # convert the datetime64 column in the dataframe to 'float days'
    daysreshape['date']=mdates.date2num(daysreshape['date'])
    # clean day data for candle view 
    daysreshape = daysreshape.reindex(columns=['date','open','high','low','close'])
    SP = len(daysreshape.date.values[0:])
    fig = plt.figure(facecolor='#07000d',figsize=(18,6))
    ax1 = plt.subplot2grid((1,10), (0,0), rowspan=1, colspan=10, facecolor='#07000d')
    #ax2 = plt.subplot2grid((1,10), (0,7), rowspan=1, colspan=3, facecolor='#07000d')
    #ax3 = plt.subplot2grid((2,3), (1,2), rowspan=1, colspan=1, facecolor='#07000d')
    generate_candle_vol(df,ax1)
   
    #generate_candle_vol(week_df,ax2)
    #generate_candle_vol(mon_df,ax3)
    #months = mdates.MonthLocator()
    #ax1.xaxis.set_major_locator(months)
    ax1.xaxis.set_major_locator(mticker.MaxNLocator(18))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%y%m%d'))
    ax1.yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
    #ax2.xaxis.set_major_locator(mticker.MaxNLocator(18))
    #ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m'))
    #ax3.xaxis.set_major_locator(months)
    #ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m'))
    plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
    ax1.yaxis.tick_right()
    ax1.yaxis.set_ticks_position('both')
    #ax2.yaxis.tick_right()
    #ax2.yaxis.set_ticks_position('both')
    #ax3.yaxis.tick_right()
    #ax3.yaxis.set_ticks_position('both')
    #plt.ylabel('Stock price and Volume')
    """
    candlestick_ohlc(ax1, daysreshape.values[-SP:], width=.6, colorup='#ff1717', colordown='#53c156')
    ax1.grid(True, color='w')
    #ax1.xaxis.set_major_locator(mticker.MaxNLocator(10))
    #wednesdays = WeekdayLocator(WEDNESDAY)
    #ax1.xaxis.set_major_locator(wednesdays)
    months = mdates.MonthLocator()
    ax1.xaxis.set_major_locator(months)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    
    ax1.yaxis.label.set_color("b")
    ax1.spines['bottom'].set_color("#5998ff")
    ax1.spines['top'].set_color("#5998ff")
    ax1.spines['left'].set_color("#5998ff")
    ax1.spines['right'].set_color("#5998ff")
    ax1.tick_params(axis='y', colors='b')
    plt.gca().yaxis.set_major_locator(mticker.MaxNLocator(prune='upper'))
    ax1.tick_params(axis='x', colors='b',rotation=270 )
    plt.ylabel('Stock price and Volume')
    volumeMin = 0
    ax1v = ax1.twinx()
    ax1v.fill_between(daysreshape.date.values[-SP:],volumeMin, df.vol.values[-SP:], facecolor='#00ffe8', alpha=.4)
    ax1v.axes.yaxis.set_ticklabels([])
    ax1v.grid(False)
    ###Edit this to 3, so it's a bit larger
    ax1v.set_ylim(0, 3*df.vol.values.max())
    ax1v.spines['bottom'].set_color("#5998ff")
    ax1v.spines['top'].set_color("#5998ff")
    ax1v.spines['left'].set_color("#5998ff")
    ax1v.spines['right'].set_color("#5998ff")
    ax1v.tick_params(axis='x', colors='b')
    ax1v.tick_params(axis='y', colors='b')
    """
    plt.savefig('Test.png')
    plt.show()  
    print (lno(),SP,daysreshape)
    #test1(df)       
   
def generate_stock_day_kline(stock_no,enddate,outf,title,df_in=pd.DataFrame()):
    if len(df_in)==0:
        df1=comm.get_stock_df_bydate_nums(stock_no,90,enddate)
    else:
        df1=df_in    
    
    #print(lno(),df1.tail())
    df1['str_date']=[comm.str_Ymd2md(x) for x in df1['date'] ]
    #df['date2'] = pd.to_datetime(df['date']).map(date2num)
    
    df1['vol'] = df1['vol'] /1000000
    format1=lambda x:"%.3f"%x
    df1["vol"]=df1["vol"].map(format1)
    #print(lno(),df1)
    #print(lno(),df1.info())
    ma_list = [5,21]
    for ma in ma_list:
        df1['MA_' + str(ma)] = df1['close'].rolling(window=ma,center=False).mean()
    df=df1.tail(60).reset_index(drop=True)
    df['dates'] = np.arange(0, len(df))
    date_tickers = df['str_date'].values
    fig = plt.figure(figsize=(10, 8))
    fig.suptitle('%s'%(title), fontsize=24, fontweight='bold')
    #sma_5 = talib.SMA(np.array(df['close']), 5)
    #sma_21 = talib.SMA(np.array(df['close']), 21)
    ax1 = fig.add_axes([0.05,0.3,0.9,0.6])
    ax2 = fig.add_axes([0.05,0.1,0.9,0.2],sharex = ax1)
    def format_date(x,pos=None):
        if x<0 or x>len(date_tickers)-1:
            return ''
        #print(x,pos)        
        #print(type(date_tickers[int(x)]))    
        return date_tickers[int(x)] 

    #ax1.xaxis.set_major_locator(mticker.MaxNLocator(5))
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    #ax1.xaxis.set_ticks_position('top')
    # 绘制K线图
    mpl_finance.candlestick_ochl( ax=ax1,quotes=df[['dates', 'open', 'close', 'high', 'low']].values,width=0.7,colorup='r',colordown='g',alpha=0.7)
    #plt.legend()
    #ax1.set_title('上证综指K线图(2017.1-)', y=0.9,fontsize=16);
    #ax1.text(0.3, 0.9, ':: %s'%stock_no, transform=ax1.transAxes, ha="right")
    #mpl_finance.volume_overlay(ax2, df['open'], df['close'], df['vol'], colorup='r', colordown='g', width=0.5, alpha=0.8)
    ax1.plot(df['MA_5'].values, label='5日均線')
    ax1.plot(df['MA_21'].values, label='21日均線')
    ax1.legend();
    #ax2.xaxis.set_major_locator(mticker.MaxNLocator(len(df['date'])))        
    ax2.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    mpl_finance.volume_overlay(ax2, df['open'], df['close'], df['vol'], colorup='r', colordown='g', width=0.7, alpha=0.8)
    """
    df['up'] = df.apply(lambda row: 1 if row['close'] >= row['open'] else 0, axis=1)
    ax2.bar(df.query('up == 1')['dates'], df.query('up == 1')['vol'], color='r', alpha=0.7)
    ax2.bar(df.query('up == 0')['dates'], df.query('up == 0')['vol'], color='g', alpha=0.7)
    """
    ax2.set_ylabel('成交量(千張)')
    plt.savefig(outf,dpi=100)
    plt.clf()
    plt.close(fig)
    #plt.show()  

    
def generate_stock_week_kline(stock_no,enddate,outf,df_in=pd.DataFrame()):
    """
    明天轉折=2天前收盤價*2減5天前收盤價。
    後天轉折=今天收盤價*2減3天前收盤價。
    大後天轉折=今天收盤價*2減三天前收盤價。
    今天轉折價=[3天前收盤價*2]減6天前收盤價 
    """
    if len(df_in)==0:
        df1=comm.get_stock_df_bydate_nums(stock_no,300,enddate)
    else:
        df1=df_in
    #print(lno(),df1['vol'].dtype)        
    cnt=60
    #print(lno(),df1.head())
    #print(lno(),len(df1))
    dist_df=tdcc_dist.get_tdcc_dist_df_bydate_num(stock_no,enddate,61)
    df1=df1.set_index('date').join(dist_df.set_index('date'))
    df1=df1.reset_index()
    df1=df1.fillna(method='ffill')
    #print(lno(),len(df1))
    df1['str_date']=[comm.str_Ymd2md(x) for x in df1['date'] ]
    df1['date']=pd.to_datetime(df1['date'])
    #print(lno(),df1['vol'].dtype)
    
    df1['vol'] = df1['vol'] /1000000
    
    #print(lno(),df1.tail())
    #print(lno(),df1.dtypes)
    #df1['vol']=df1.round({'外資': 1, '投信': 1, '自營商': 1})
    week_df=resample(df1,'W',cnt)
    #print(lno(),len(week_df))
    format1=lambda x:"%.2f"%x
    week_df["vol"]=week_df["vol"].map(format1)
    week_df["date"]=week_df["date"].apply(np.datetime64)
    #
    #week_df['vol']=week_df.round({'vol': 2})
    #print(lno(),week_df.columns)
    #print(lno(),week_df.info())
    #print(lno(),dist_df.info())
    """
    ma_list = [4,18]
    for ma in ma_list:
        week_df['MA_' + str(ma)] = week_df['close'].rolling(window=ma,center=False).mean()
    """
    df=week_df.tail(cnt).reset_index(drop=True)
    df=tdcc_dist.tdcc_pwr_calc(df)
    
    df['dates'] = np.arange(0, len(df))
    date_tickers = df['str_date'].values
    fig = plt.figure(figsize=(10, 8))
    tt=eps.gen_eps_river(stock_no,enddate)

    str_tt=''
    if len(tt)==6:
        str_tt='eps:%.2f 本益比 天(%.2f)高(%.2f)合理(%.2f)建議買(%.2f)地板(%.2f)'%(tt[0],tt[1],tt[2],tt[3],tt[4],tt[5])
    fig.suptitle('%s'%(str_tt), fontsize=16, fontweight='bold')

    
    #sma_5 = talib.SMA(np.array(df['close']), 5)
    #sma_21 = talib.SMA(np.array(df['close']), 21)
    ax1 = fig.add_axes([0.05,0.25,0.9,0.5])
    ax2 = fig.add_axes([0.05,0.1,0.9,0.15],sharex = ax1)
    ax3 = fig.add_axes([0.05,0.75,0.9,0.15],sharex = ax1)
    def format_date(x,pos=None):
        if x<0 or x>len(date_tickers)-1:
            return ''
        #print(x,pos)        
        #print(type(date_tickers[int(x)]))    
        return date_tickers[int(x)]

    #ax1.xaxis.set_major_locator(mticker.MaxNLocator(5))
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    #ax1.xaxis.set_ticks_position('top')
    # 绘制K线图
    mpl_finance.candlestick_ochl(ax=ax1,quotes=df[['dates', 'open', 'close', 'high', 'low']].values,width=0.7,colorup='r',colordown='g',alpha=0.7)
    if tt[2]>0 and tt[0]>0 and tt[2]<48:
        ypos=tt[0]*tt[2]
        ax1.axhline(y=ypos,xmin=0.75, xmax=1,color= 'r', linestyle= '--')
    if tt[3]>0 and tt[0]>0 and tt[3]<35:
        ypos=tt[0]*tt[3]
        ax1.axhline(y=ypos,xmin=0.75, xmax=1,color= 'b', linestyle= '--')
    if tt[4]>0 and tt[0]>0 and tt[4]<30:
        ypos=tt[0]*tt[4]
        ax1.axhline(y=ypos,xmin=0.75, xmax=1,color= 'g', linestyle= '--')
    #plt.legend()
    #ax1.set_title('上证综指K线图(2017.1-)', y=0.9,fontsize=16);
    #ax1.text(0.3, 0.9, ':: %s'%stock_no, transform=ax1.transAxes, ha="right")
    #mpl_finance.volume_overlay(ax2, df['open'], df['close'], df['vol'], colorup='r', colordown='g', width=0.5, alpha=0.8)
    #ax1.plot(df['MA_5'].values, label='5日均線')
    #ax1.plot(df['MA_21'].values, label='21日均線')
    #ax1.legend();
    #ax2.xaxis.set_major_locator(mticker.MaxNLocator(len(df['date'])))        
    ax2.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    mpl_finance.volume_overlay(ax2, df['open'], df['close'], df['vol'], colorup='r', colordown='g', width=0.7, alpha=0.7)
    print(lno(),df[['date','tdcc_pwr']])
    ax3.bar(df.query('tdcc_pwr > 0')['dates'], df.query('tdcc_pwr > 0')['tdcc_pwr'], color='r', alpha=0.7)
    ax3.bar(df.query('tdcc_pwr <= 0')['dates'], df.query('tdcc_pwr <= 0')['tdcc_pwr'], color='g', alpha=0.7)
    #A=ax3.bar(df['dates'],df['tdcc_pwr'],color='r',width=0.7, alpha=0.7)
    def createLabels(data):
        for item in data:
            height = item.get_height()
            ax3.text(
                item.get_x()+item.get_width()/2., 
                height*1.05, 
                '%.3f' % height,
                ha = "center",
                va = "bottom",
            )
    #createLabels(A)
    """
    df['up'] = df.apply(lambda row: 1 if row['close'] >= row['open'] else 0, axis=1)
    ax2.bar(df.query('up == 1')['dates'], df.query('up == 1')['vol'], color='r', alpha=0.7)
    ax2.bar(df.query('up == 0')['dates'], df.query('up == 0')['vol'], color='g', alpha=0.7)
    """
    ax2.set_ylabel('成交量(千張)')
    plt.savefig(outf,dpi=100)
    plt.clf()
    plt.close(fig)
    #plt.show()  

def generate_stock_kline_pic(stock_no,enddate,outf,df_in=pd.DataFrame()):
    if len(df_in)==0:
        dfs=comm.get_stock_df_bydate_nums(stock_no,220,enddate)
    else:
        dfs=df_in    
    #print(lno(),df1.tail())
    dfs['str_date']=[comm.str_Ymd2md(x) for x in dfs['date'] ]
    
    df=dfs.tail(90).reset_index(drop=True).copy()
    #print(lno(),df)
    df['vol'] = df['vol'] /1000000
    format1=lambda x:"%.3f"%x
    df["vol"]=df["vol"].map(format1)
    #print(lno(),df1)
    #print(lno(),df1.info())
    #df=df1.tail(90).reset_index(drop=True).copy()
    ma_list = [5,21]
    for ma in ma_list:
        df['MA_' + str(ma)] = df['close'].rolling(window=ma,center=False).mean()
    df=df.tail(40).reset_index(drop=True)
    df['dates'] = np.arange(0, len(df))
    date_tickers = df['str_date'].values
    fig = plt.figure(figsize=(10, 10))
    fig.suptitle('%s %s'%(stock_no,comm.get_name_by_stock_id(stock_no)), fontsize=20, fontweight='bold')
    ax1 = fig.add_axes([0.1,0.6,0.9,0.3])
    ax2 = fig.add_axes([0.1,0.5,0.9,0.1],sharex = ax1)
    ax3 = fig.add_axes([0.1,0.15,0.9,0.3])
    ax4 = fig.add_axes([0.1,0.05,0.9,0.1],sharex = ax3)
    def format_date(x,pos=None):
        if x<0 or x>len(date_tickers)-1:
            return ''
        return date_tickers[int(x)] 

    #ax1.xaxis.set_major_locator(mticker.MaxNLocator(5))
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    #ax1.xaxis.set_ticks_position('top')
    # 绘制K线图
    mpl_finance.candlestick_ochl(ax=ax1,quotes=df[['dates', 'open', 'close', 'high', 'low']].values,width=0.7,colorup='r',colordown='g',alpha=0.7)
    
    ax1.plot(df['MA_5'].values, label='5日均線')
    ax1.plot(df['MA_21'].values, label='21日均線')
    ax1.set_title('日K',loc='left')
    ax1.legend();
    #ax2.xaxis.set_major_locator(mticker.MaxNLocator(len(df['date'])))        
    ax2.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    #ax2.xaxis.set_ticks_position('none')
    mpl_finance.volume_overlay(ax2, df['open'], df['close'], df['vol'], colorup='r', colordown='g', width=0.7, alpha=0.7)
    
    ax2.set_ylabel('成交量(千張)')
    cnt=40
    df1=dfs
    df1['date']=pd.to_datetime(df1['date'])
    #df1['vol']=df1.round({'外資': 1, '投信': 1, '自營商': 1})
    week_df=resample(df1,'W',cnt)
    format1=lambda x:"%.2f"%x
    week_df["vol"]=week_df["vol"].map(format1)
    #week_df['vol']=week_df.round({'vol': 2})
    print(lno(),week_df.tail())
    #print(lno(),df1.info())

    df=week_df.tail(cnt).reset_index(drop=True).copy()
    df['dates'] = np.arange(0, len(df))
    #print(lno(),df)
    date_tickers1 = df['str_date'].values
    print(lno(),date_tickers1)
    
    def format_date1(x,pos=None):
        if x<0 or x>len(date_tickers1)-1:
            return ''
        return date_tickers1[int(x)]

    #ax1.xaxis.set_major_locator(mticker.MaxNLocator(5))
    ax3.xaxis.set_major_formatter(ticker.FuncFormatter(format_date1))
    ax3.xaxis.set_ticks_position('none')
    ax3.set_title('周K',loc='left')
    # 绘制K线图
    mpl_finance.candlestick_ochl(ax=ax3,
    quotes=df[['dates', 'open', 'close', 'high', 'low']].values,width=0.7,colorup='r',colordown='g',alpha=0.7)
    
    ax4.xaxis.set_major_formatter(ticker.FuncFormatter(format_date1))
    mpl_finance.volume_overlay(ax4, df['open'], df['close'], df['vol'], colorup='r', colordown='g', width=0.7, alpha=0.7)

    """
    df['up'] = df.apply(lambda row: 1 if row['close'] >= row['open'] else 0, axis=1)
    ax4.bar(df.query('up == 1')['dates'], df.query('up == 1')['vol'], color='r', alpha=0.7)
    ax4.bar(df.query('up == 0')['dates'], df.query('up == 0')['vol'], color='g', alpha=0.7)
    """
    ax4.set_ylabel('成交量(千張)')
    plt.savefig(outf)
    plt.clf()
    plt.close(fig)

def show_stock_kline_pic(stock_no,enddate,cnt):
    stk=comm.stock_data()
    dfs=stk.get_df_by_enddate_num(stock_no,enddate+relativedelta(months=4),cnt+75)
    #dfs=df_in    
    dfs['str_date']=[comm.str_Ymd2md(x) for x in dfs['date'] ]
    
    df=dfs.reset_index(drop=True).copy()
    #print(lno(),df)
    index=df[(df.loc[:,"date"] <= enddate)].index[0]
    df['vol'] = df['vol'] /1000000
    format1=lambda x:"%.3f"%x
    df["vol"]=df["vol"].map(format1)
    
    ma_list = [5,10,20]
    for ma in ma_list:
        df['MA_' + str(ma)] = df['close'].rolling(window=ma,center=False).mean()
    df['dates'] = np.arange(0, len(df))
    date_tickers = df['str_date'].values
    fig = plt.figure(figsize=(16, 5))
    fig.suptitle('%s %s'%(stock_no,comm.get_name_by_stock_id(stock_no)), fontsize=20, fontweight='bold')
    ax1 = fig.add_axes([0.1,0.3,0.9,0.6])
    ax2 = fig.add_axes([0.1,0.1,0.9,0.2],sharex = ax1)
    #ax3 = fig.add_axes([0.1,0.15,0.9,0.3])
    #ax4 = fig.add_axes([0.1,0.05,0.9,0.1],sharex = ax3)
 
    def format_date(x,pos=None):
        if x<0 or x>len(date_tickers)-1:
            return ''
        return date_tickers[int(x)] 
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    # 绘制K线图
    mpl_finance.candlestick_ochl(ax=ax1,quotes=df[['dates', 'open', 'close', 'high', 'low']].values,width=0.7,colorup='r',colordown='g',alpha=0.7)
    
    ax1.plot(df['MA_5'].values, label='5日均線')
    ax1.plot(df['MA_10'].values, label='10日均線')
    ax1.plot(df['MA_20'].values, label='20日均線')
    ax1.set_title('日K',loc='left')
    ax1.legend();
    x=df.close.values
    peaks, _ = signal.find_peaks(x,prominence=df.iloc[-1]['close']*0.08)
    print(lno(),peaks)
    ax1.plot(peaks, x[peaks], "x")
    index=df[(df.loc[:,"date"] <= enddate)].index[-1]
    print(lno(),index)
    ax1.axvline(index, 0.2, 0.8, linestyle= '--')

    #ax2.xaxis.set_major_locator(mticker.MaxNLocator(len(df['date'])))        
    ax2.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    #ax2.xaxis.set_ticks_position('none')
    mpl_finance.volume_overlay(ax2, df['open'], df['close'], df['vol'], colorup='r', colordown='g', width=0.7, alpha=0.7)
    
    ax2.set_ylabel('成交量(千張)')
    
    df2=stk.get_df_by_startdate_enddate(stock_no,enddate,enddate+relativedelta(days=100))
    #dfs=df_in    
    #print(lno(),df1.tail())
    df2['str_date']=[comm.str_Ymd2md(x) for x in df2['date'] ]
    
    df2=df2.reset_index(drop=True).copy()
    #print(lno(),df)
    df2['vol'] = df2['vol'] /1000000
    format1=lambda x:"%.3f"%x
    df2["vol"]=df2["vol"].map(format1)
    df2['dates'] = np.arange(0, len(df2.index))
    date_tickers2 = df2['str_date'].values
    """
    ax3 = fig.add_axes([0.1,0.15,0.9,0.3])
    ax4 = fig.add_axes([0.1,0.05,0.9,0.1],sharex = ax3)
 
    def format_date2(x,pos=None):
        if x<0 or x>len(date_tickers2)-1:
            return ''
        return date_tickers2[int(x)] 
    ax3.xaxis.set_major_formatter(ticker.FuncFormatter(format_date2))
    # 绘制K线图
    mpl_finance.candlestick_ochl(ax=ax3,quotes=df2[['dates', 'open', 'close', 'high', 'low']].values,width=0.7,colorup='r',colordown='g',alpha=0.7)
    ax3.set_title('日K',loc='left')
    ax3.legend();
    ax4.xaxis.set_major_formatter(ticker.FuncFormatter(format_date2))
    mpl_finance.volume_overlay(ax4, df2['open'], df2['close'], df2['vol'], colorup='r', colordown='g', width=0.7, alpha=0.7)
    
    ax4.set_ylabel('成交量(千張)')
    """
    plt.show()  
import scipy.signal as signal    
def show_stock_kline_pic_by_df(df_in):
    dfs=df_in
    #print(lno(),df1.tail())
    dfs['str_date']=[comm.str_Ymd2md(x) for x in dfs['date'] ]
    df=dfs.reset_index(drop=True).copy()
    #print(lno(),df)
    df['vol'] = df['vol'] /1000000
    format1=lambda x:"%.3f"%x
    df["vol"]=df["vol"].map(format1)
    #print(lno(),df1)
    #print(lno(),df1.info())
    #df=df1.tail(90).reset_index(drop=True).copy()
    ma_list = [5,10,20]
    for ma in ma_list:
        df['MA_' + str(ma)] = df['close'].rolling(window=ma,center=False).mean()
    df['dates'] = np.arange(0, len(df))
    date_tickers = df['str_date'].values
    fig = plt.figure(figsize=(10, 5))
    ax1 = fig.add_axes([0.1,0.6,0.9,0.3])
    ax2 = fig.add_axes([0.1,0.5,0.9,0.1],sharex = ax1)
 
    def format_date(x,pos=None):
        if x<0 or x>len(date_tickers)-1:
            return ''
        return date_tickers[int(x)] 
    ax1.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    # 绘制K线图
    mpl_finance.candlestick_ochl(ax=ax1,quotes=df[['dates', 'open', 'close', 'high', 'low']].values,width=0.7,colorup='r',colordown='g',alpha=0.7)
    
    ax1.plot(df['MA_5'].values, label='5日均線')
    ax1.plot(df['MA_10'].values, label='10日均線')
    ax1.plot(df['MA_20'].values, label='20日均線')
    ax1.set_title('日K',loc='left')
    ax1.legend();
    """
    x=df.close.values
    peaks, _ = signal.find_peaks(x,prominence=df.iloc[-1]['close']*0.025, distance=2)
    ax1.plot(peaks, x[peaks], "x")
    """
    #ax2.xaxis.set_major_locator(mticker.MaxNLocator(len(df['date'])))        
    ax2.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    #ax2.xaxis.set_ticks_position('none')
    mpl_finance.volume_overlay(ax2, df['open'], df['close'], df['vol'], colorup='r', colordown='g', width=0.7, alpha=0.7)
    
    ax2.set_ylabel('成交量(千張)')
    plt.show()  

if __name__ == '__main__':
    sns.set()
    mpl.rcParams['font.family'] = 'sans-serif'
    mpl.rcParams['font.sans-serif'] = 'SimHei'
  #df=kline.get_tse_kline_df(startdate,enddate)
    if sys.argv[1]=='1' :
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        selectdate=startdate
        #df=get_tse_df_from_csv(startdate,enddate) 
        stock_no='6152'
        mode=21
        filen='out/%s_%d.png'%(stock_no,mode)
        test2(stock_no,enddate,21,filen)
        test3(stock_no,enddate,filen)
        
    elif sys.argv[1]=='-d' :
        ##download 大盤指數 outf csv/tse_kline
        print (lno(),len(sys.argv))
        if len(sys.argv)==2 :
            today= datetime.today().date()
            enddate=datetime(year=today.year,month=today.month,day=today.day,)
            startdate=enddate- relativedelta(months=1)

        else :
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        down_tse_kline(startdate,enddate)
        #parse_tse_kline(startdate,enddate)
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
            
            generate_stock_kline_pic(stock_no,enddate,'test1.png')
            #generate_stock_week_kline(stock_no,enddate,'test1.png')

        else :
            print (lno(),'func -t stock_no date')  
    
