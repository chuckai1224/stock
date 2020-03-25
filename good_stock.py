# -*- coding: utf-8 -*-
#from __future__ import unicode_literals
import io
import csv
import os
import time
import timeit
import sys
#import urllib2
from datetime import datetime
from dateutil.relativedelta import relativedelta
try:
    import multiprocessing 
    from pandarallel import pandarallel
    #pandarallel.initialize()
    #from multiprocessing import Pool
    #from pathos.multiprocessing import ProcessingPool as Pool
    #n_processes = 4  # My machine has 4 CPUs
    #pool = Pool(processes=n_processes)
    pass
except:
    pass    
import stock_comm as comm 
import stock_big3
import tdcc_dist
import requests
import inspect
from inspect import currentframe, getframeinfo
import pandas as pd
import numpy as np
import op
import math
import kline
from sqlalchemy import create_engine
#from pyecharts import Kline
#from pyecharts import Candlestick
#import webbrowser
import revenue
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from time import sleep
from selenium.webdriver.support.ui import Select
import shutil
import talib
from talib import abstract
import scipy.signal as signal 
from stocktool import comm as cm1 
import platform
import all_stock
import seaborn as sns
import matplotlib as mpl
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)

def check_dst_folder(dstpath):
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath)     


                
def down_data(enddate,download=1):

    dst_folder='%s\\data\\good_stock'%(os.getcwd())
    #dst_folder='%s'%(os.getcwd())
    filename='%s/tmp.csv'%(dst_folder)
    out_file='data/good_stock/%d%02d%02d.csv'%(int(enddate.year), int(enddate.month),int(enddate.day))
    check_dst_folder(dst_folder)
    enddate_str='%d/%02d/%02d'%(int(enddate.year), int(enddate.month),int(enddate.day))
    """
    fp = webdriver.FirefoxProfile()
    fp.set_preference("browser.download.folderList",2)
    fp.set_preference("browser.download.manager.showWhenStarting",False)
    fp.set_preference("browser.download.dir", os.getcwd())
    #fp.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream")
    fp.set_preference("browser.helperApps.neverAsk.saveToDisk","application/xls;text/csv");
    """
    url = 'https://www.sitca.org.tw/ROC/Industry/IN2629.aspx?pid=IN22601_04'
    profile = webdriver.FirefoxProfile()
    print(lno(),dst_folder)
    profile.set_preference('browser.download.dir', dst_folder)
    profile.set_preference('browser.download.folderList', 2)
    profile.set_preference('browser.download.manager.showWhenStarting', False)
    profile.set_preference('browser.helperApps.alwaysAsk.force', False);
    profile.set_preference('browser.download.manager.showWhenStarting',False);
    profile.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/xls,application/octet-stream,application/vnd.ms-excel')
    driver = webdriver.Firefox(firefox_profile=profile)
    driver.implicitly_wait(8) # 隱式等待
    driver.get(url)
    sleep(3)
    driver.find_element_by_id("ctl00_ContentPlaceHolder1_rbClass").send_keys(Keys.SPACE)
    sleep(5)
    if driver.find_element_by_id("ctl00_ContentPlaceHolder1_rbClass").is_selected():
        print(lno(),'selected!')
    else:
        print(lno(), 'not yet!')
    #driver.find_element_by_id("ctl00_ContentPlaceHolder1_BtnQuery").send_keys(Keys.SPACE)
    driver.find_element_by_id("ctl00_ContentPlaceHolder1_BtnExport").send_keys(Keys.SPACE)
    
    sleep(5)
    #driver.find_element_by_id("ctl00_ContentPlaceHolder1_BtnQuery").click()
    #driver.find_element_by_id("ctl00_ContentPlaceHolder1_ddlQ_Class").send_keys(Keys.SPACE)
    #select = Select(driver.find_element_by_id("ctl00_ContentPlaceHolder1_ddlQ_Class"))
    #select.select_by_index(0)
    #select.select_by_value("op_2")
    #select.select_by_value("op_2")
    filename1 = max([dst_folder + "\\" + f for f in os.listdir(dst_folder)],key=os.path.getctime)
    print(lno(),filename1)
    #shutil.move(filename,os.path.join(dst_folder,r"newfilename.ext"))
    html = driver.page_source       # get html
    #print(lno(),html)
    driver.get_screenshot_as_file("./img/sreenshot1.png")
    driver.close()
    return
    """
    ctl00$ContentPlaceHolder1$ddlQ_YM	201908
    ctl00$ContentPlaceHolder1$rdo1	rbClass
    ctl00$ContentPlaceHolder1$ddlQ_Class	AA1
    ctl00$ContentPlaceHolder1$BtnQuery	查詢
    """
    query_params = {
        'queryType': '2',
        'marketCode': '0',
        'dateaddcnt':'',
        'commodity_id':'TX',
        'commodity_id2':'',
        'queryDate': enddate_str,
        'MarketCode':'0','commodity_idt':'TX','commodity_id2t':'','commodity_id2t2':''
        
    }
    #print(lno(),download)
    if download==1:
        #print(lno(),url,query_params)
        page = requests.post(url, data=query_params)

        if not page.ok:
            print(lno(),"Can not get data at {}".format(url))
            return 
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in page:
                file.write(chunk)
    if not os.path.exists(filename): 
        return
    
    dfs = pd.read_html(filename,encoding = 'utf8')
    #print(lno(),len(dfs))
    for df in dfs :
        #print(lno(),df.iloc[0].values.tolist())
        if '契約' in df.iloc[0].values.tolist():
            columns=df.iloc[0].values.tolist()
            df.columns=columns
            
            df.dropna(axis=1,how='all',inplace=True)
            df.dropna(inplace=True)
            df=df.drop([0]).reset_index(drop=True)
            df.to_csv(out_file,encoding='utf-8', index=False)
            

def generate_final(enddate):
    dst_folder='data/fut/final'
    check_dst_folder(dst_folder)
    out_file='data/fut/final/fut.csv'
    add_file='data/fut/%d%02d%02d.csv'%(int(enddate.year), int(enddate.month),int(enddate.day))
    
    if os.path.exists(add_file): 
        dfs = pd.read_csv(add_file,encoding = 'utf-8',dtype= {'公司代號':str})
        dfs.dropna(axis=1,how='all',inplace=True)
        dfs.dropna(inplace=True)
        #dfs.columns=['公司代號','公司名稱','本季eps','本期綜合損益總額','毛利率','營利率','純益率']
        df=dfs.head(1).copy()
        str_date='%d/%02d/%02d'%(int(enddate.year), int(enddate.month),int(enddate.day))
        df['日期']=str_date
        df['日期']=[comm.date_sub2time64(x) for x in df['日期'] ]  
        df=df[['日期','最後成交價']]
        print(lno(),df)
        if os.path.exists(out_file): 
            df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype= {'日期':str})
            df_s['日期']=[comm.date_sub2time64(x) for x in df_s['日期'] ]  
            df_s=df_s.append(df,ignore_index=True)
            df_s.dropna(axis=1,how='all',inplace=True)
            df_s.dropna(inplace=True)
            df_s.drop_duplicates(subset=['日期'],keep='last',inplace=True)
            df_s=df_s.sort_values(by=['日期'], ascending=False)
            #print(lno(),df_s)
            df_s.to_csv(out_file,encoding='utf-8', index=False)
        
        else :
            df.to_csv(out_file,encoding='utf-8', index=False)
        
def down_gen_datas(startdate,enddate):
    #startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
    day=0
    nowdate=enddate
    while   nowdate>=startdate :
        nowdate = enddate - relativedelta(days=day)
        down_data(nowdate) 
        generate_final(nowdate) 
        day=day+1   
def get_df_bydate(date,debug=0):
    out_file='data/fut/final/fut.csv'
    #print(lno(),date)
    
    outcols=['日期','最後成交價']
    if os.path.exists(out_file): 
        date_str='%d-%02d-%02d'%(int(date.year), int(date.month),int(date.day))
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print(lno(),df_s[(df_s['date'] == date_str)].values.tolist())
        df=df_s[(df_s['日期'] == date_str)]
        if len(df)==1:
            df['日期']=[comm.date_sub2time64(x) for x in df['日期'] ]  
            df.reset_index(inplace=True)
            if debug==1:
                print(lno(),df)
            return df
        
        return pd.DataFrame(pd.np.empty(( 1, len(outcols))) * pd.np.nan, columns = outcols)
        
    else :
        return pd.DataFrame(pd.np.empty(( 1, len(outcols))) * pd.np.nan, columns = outcols)

def get_dfs_bydate(startdate,enddate,debug=0):
    out_file='data/fut/final/fut.csv'
    #print(lno(),date)
    outcols=['日期','最後成交價']
    if os.path.exists(out_file): 
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        df_s['日期']=[comm.date_sub2time64(x) for x in df_s['日期'] ]  
        #print(lno(),df_s[(df_s['date'] == date_str)].values.tolist())
        df=df_s[(df_s.loc[:,"日期"] <= np.datetime64(enddate))&(df_s.loc[:,"日期"] >= np.datetime64(startdate))].copy()
        
        if len(df)!=0:
            df=df.reset_index(drop=True)
            if debug==1:
                print(lno(),df)
            return df
        
        return pd.DataFrame(pd.np.empty(( 1, len(outcols))) * pd.np.nan, columns = outcols)
        
    else :
        return pd.DataFrame(pd.np.empty(( 1, len(outcols))) * pd.np.nan, columns = outcols)
def get_good_stock_m1(selday,debug=0):
    """
    1. 投信 小市值
    市值（百萬） < 10000
    投信當日買賣 > 200 張
    """
    sb3=get_stock_big3()
    stock_3big_df=sb3.get_df_by_date(selday)
    if len(stock_3big_df)==0:
        return pd.DataFrame()
    df= stock_3big_df[(stock_3big_df['投信買賣超股數'] >= 200000)].copy()
    #print(lno(),df)
    def check_no_need(r):
        if len(r['證券代號'])!=4:
            return 0
        if r['證券代號'].startswith( '0' ):
            return 0
        return   1  
    df['result']=df.apply(check_no_need,axis=1)
    df_out= df[(df['result'] == 1)].reset_index(drop=True)
    df_out['證券名稱']=df_out['證券名稱'].str.strip()
    df_out=df_out[['date','證券代號','證券名稱','market','外陸資買賣超股數(不含外資自營商)','投信買賣超股數','自營商買賣超股數','三大法人買賣超股數']]
    df_out.columns=['date','stock_id','stock_name','market','外陸資買賣超股數(不含外資自營商)','投信買賣超股數','自營商買賣超股數','三大法人買賣超股數']
    #print(lno(),df_out)
    return df_out

def get_fund_buy_history():
    """
    ##TODO:  莊家偵測　
    #1. Ｋ線買力＞５日均　３倍
    #2. Ｋ線買力＞10日均　３倍
    3. Ｋ線買力＞60日前１０
    4.投信買超
    5.千張大戶買超
    6.４００張以上大戶買超
    
    """
    engine = create_engine('sqlite:///sql/buy_signal.db', echo=False)
    con = engine.connect()
    startdate=datetime(2013, 1, 1)
    #startdate=datetime(2019, 1, 1)
    enddate=datetime(2019, 11, 30)
    nowdate=startdate
    df_fin=pd.DataFrame()
    while nowdate<enddate:
        nowdate=nowdate+relativedelta(days=1)
        df1=get_good_stock_m1(nowdate)
        if len(df1.index):
            df_fin=pd.concat([df_fin,df1])
    df_fin.to_sql(name='find_fund_buy_v2', con=con, if_exists='replace', index=False,chunksize=10)       
    print(lno(),df_fin)
    
    #d=comm.get_tse_exchange_data(selday)
def get_date_income_ratio(r):
    ratio1=np.nan
    result=np.nan
    income=get_sql_income()
    date=r.date-relativedelta(months=1)
    df1=income.get_by_date(date)
    #print(lno(),df1)
    #raise
    if not r.stock_id in df1['公司代號'].values.tolist():
        return ratio1,result
    #print(lno(),r.stock_id)
    df =income.get_by_stockid_date_months(r.stock_id,date,12) 
    if len(df.index)>=1:
        df['當月營收']=df['當月營收'].astype(float)
        df['去年當月營收']=df['去年當月營收'].astype(float)
        YOY=df.iloc[0]['去年同月增減(%)']
        if  len(df.index)>=12:  
            try:
                df['inavg3']=df['當月營收'].rolling(window=3,center=False,axis=0).mean()
                df['inavg12']=df['當月營收'].rolling(window=12,center=False,axis=0).mean()
                avg3=df.iloc[-1]['inavg3']
                avg12=df.iloc[-1]['inavg12']
                ratio1=avg3/avg12
            except:
                print(lno(),r.stock_id,df)
                pass
  
            try:
                df['inavg4']=df['當月營收'].rolling(window=4,center=False,axis=0).mean()
                length=len(df.index)
                result=1
                #print(lno(),df)
                for i in range(0,5):
                    #print(lno(),i,df.iloc[-1-i]['inavg4'],df.iloc[-2-i]['inavg4'])
                    if df.iloc[-1-i]['inavg4']<=df.iloc[-2-i]['inavg4']:
                        result=0
                        break
            except:        
                print(lno(),r.stock_id,df)
                pass
        #print(df['當月營收'].sum(),df['去年當月營收'].sum())
        #print(df)
        #print(lno(),ratio1,ratio2)
    #sys.exit()    
    return ratio1,result   
# 高成長 莊家信號     
def gen_buy_list(date):
    engine = create_engine('sqlite:///sql/revenue_good.db', echo=False)
    con = engine.connect()
    markets=['tse','otc']
    d1=comm.exchange_data('tse').get_df_date_parse(date)
    d1['market']='tse'
    d2=comm.exchange_data('otc').get_df_date_parse(date)
    d2['market']='otc'
    d=pd.concat([d1,d2])
    if len(d.index)==0:
        return 
    #
    d[['月營收均線法','月營收成長法']]=d.apply(get_date_income_ratio,axis=1,result_type="expand")  
    #d1=d[(d['月營收均線法']>=1)|(d['月營收成長法']>=1) |(d['投信買超']>=200000) ].reset_index(drop=True)
    d1=d[(d['月營收均線法']>=1)|(d['月營收成長法']>=1)  ].reset_index(drop=True)

    #d['投信買超']=d.apply(find_fund_buy,axis=1)  
    #d1=data[(data[var1]==1)&(data[var2]>10)]
    #d['投信買超']=d.apply(find_fund_buy,axis=1)  
    print(lno(),d1)
    if len(d1.index):
        table_name='revenue_good_{}'.format(date.strftime('%Y%m%d'))
        d1.to_sql(name=table_name, con=con, if_exists='replace', index=False,chunksize=10)  
    
    
    #if len(df1.index):
    #    df_fin=pd.concat([df_fin,df1])
    #df_fin.to_sql(name='find_fund_buy_v2', con=con, if_exists='replace', index=False,chunksize=10)       
    #print(lno(),df_fin)
def get_market_value(r):
    #return 億
    date=r.date
    stock_id=r.stock_id
    tdcc=get_tdcc_dist() 
    total_stock_nums=tdcc.get_total_stock_num(stock_id,date)
    if total_stock_nums==0:
        return 
    stk=get_stock_data()    
    df=stk.get_df_by_startdate_enddate(stock_id,date,date+relativedelta(days=1))    
    if len(df.index)==0:
        return 
    #print(lno(),r.stock_id,total_stock_nums)        
    #print(lno(),df)    
    return  df.iloc[0]['close']* total_stock_nums /100000000
def find_fund_buy_fix(r):
    try :
        sb3=get_stock_big3()
        df=sb3.get_df_by_id_date(r.stock_id,r.date)
        #print(lno(),df)
        if len(df.index)==0:
            return 
        else:    
            #out=df[['外陸資買賣超股數(不含外資自營商)','投信買賣超股數','自營商買賣超股數','三大法人買賣超股數']].values[0].tolist()
            fund_buy=df.iloc[0]['投信買賣超股數']
            #print(lno(),fund_buy)
            if fund_buy>200*1000:
                return fund_buy
            return 
    except:
        print(lno(),'find_fund_buy_fix error')
        return
def check_skip_stock(r):
    close=np.nan
    if len(r.stock_id)!=4:
        return close 
    stk=get_stock_data()  
    df=stk.get_df_by_startdate_enddate(r.stock_id,r.date,r.date+relativedelta(days=1))    
    if len(df.index)==0:
        return close
    r1=df.iloc[0]
    #print(r1)
    cash=float(r1.cash)
    if cash<3000000:
        return close
    return r1.close   
def check_point_K(r):
    stk=get_stock_data()
    df1=stk.get_df_by_enddate_num(r.stock_id,r.date,122)
    #print(lno(),df1)
    if len(df1)<90:
        return
    df1['red_K_ratio']=df1.apply(red_K_ratio_calc, axis=1)
    top10=df1.sort_values(by='red_K_ratio',ascending=False).iloc[10]['red_K_ratio']
    #print(lno(),df1,top10)
    if df1.iloc[-1]['red_K_ratio']>=top10:
        return 1
    return      
def gen_buy_list_step1(date):
    ## 抓取營收 
    ## 莊家信號  投信小主力  強力K
    engine = create_engine('sqlite:///sql/revenue_good.db', echo=False)
    con = engine.connect()
    table_name='revenue_good_{}'.format(date.strftime('%Y%m%d'))
    cmd='SELECT * FROM "{}" '.format(table_name)
    df = pd.read_sql(cmd, con=con, parse_dates=['date']) 
    df['date']=date
    df['close']=df.apply(check_skip_stock,axis=1)  
    
    df = df[~df['close'].isnull()]
    df['投信買超']=df.apply(find_fund_buy_fix,axis=1)  
    df['市值']=df.apply(get_market_value,axis=1)
    df['point_K']=df.apply(check_point_K,axis=1)
    df[['大戶買超','中戶買超']]=df.apply(get_tdcc_dist_1000_400_buy_vol,axis=1,result_type="expand")
    df['400張以上買超']=df['大戶買超']+df['中戶買超']
    #"""
    d1=df[(df['投信買超']>=200000)&(df['市值']<100)  ].reset_index(drop=True)
    if len(d1.index):
        all_stock.generate_stock_html_mode2(date,mode='營收投信買超',in_df=[d1])
    print(lno(),d1)
    #"""
    d2=df[(df['point_K']>=1)&(df['400張以上買超']>=400000)&(df['市值']<100)  ].reset_index(drop=True)
    if len(d2.index):
        all_stock.generate_stock_html_mode2(date,mode='營收關鍵K大戶買超',in_df=[d2])
    print(lno(),d2)

class findstock:
    def __init__(self):
        #self.rundate=rundate
        #self.strategy=strategy
        self.tse=comm.exchange_data('tse')
        self.otc=comm.exchange_data('otc')
        self.stk=comm.stock_data()
        #self.market=market
        
        method='buy_signal'
        self.engine = create_engine('sqlite:///sql/%s.db'%method, echo=False)
        self.con = self.engine.connect() 
        self.cmp_df=pd.DataFrame()          
        self.tdcc=tdcc_dist.tdcc_dist()          
    def check_long_candle(self,df_o):  
        def calc_klinebody(row):
            if row['diff']>0 and row['open'] >(row['close']-row['diff']):
                real_body= row['diff']
            elif row['diff']<0 and row['open'] <(row['close']-row['diff']):
                real_body= -row['diff']
            else:
                real_body= abs(row['close']-row['open'])
            return real_body    
        df=df_o.copy()
        df['klinebody']=df.apply(calc_klinebody,axis=1)
        period=len(df)-1
        #print(lno(),df)
        df_box=df.iloc[-period-1:-1]
        #print(lno(),df_box)
        avg=df_box['klinebody'].mean()
        high=df_box['high'].max()
        low=df_box['low'].min()
        return df.iloc[-1]['klinebody']/avg,high,low
    def find_stong_red_stk(self,r):
        if comm.check_stock_id(r.stock_id)==False:
            return 
        #print(lno(),r)
        if float(r.vol)<1000:  # 交易量 小於一張
            return 
        if type( r['open'])==str and '--' in r['open']:  # -- 不處理
            return 
        if r.open is None:
            return  
        try:         
            open=float(r.open)
            close=float(r.close)
        except:
            print(lno(),r)
            raise    
        if open >= close:  # 非紅K
            return
        
        try:
            diff=float(r['diff'])
        except:
            print(lno(),type(r['diff']),r['diff'])
            raise    
        
        pre_close=close-diff
        #print(lno(),open,close,pre_close)
        if open >pre_close:
            LongRedK_ratio= (close -pre_close)/pre_close
        else :
            LongRedK_ratio= (close -open)/pre_close
        #print(lno(),type(close),type
        if LongRedK_ratio<0.03:  #紅K 比例太小
            return
        
        box_date=self.boxdate
        startdate=self.rundate-relativedelta(months=5)
        ##need rundate data 
        enddate=self.rundate+relativedelta(days=1)
            
        df_all=self.stk.get_df_by_startdate_enddate(r.stock_id,startdate,enddate)

        if len(df_all)<(box_date+1):
            return
        #df_all=comm.get_stock_df(stokc_id)
        #print(lno(),df_all.open.values)
        df=df_all.tail(box_date+1)
        try:
            long_ratio,box_high,box_low=cm1.get_long_red_ratio(df.open.values,df.high.values,df.low.values,df.close.values,df['diff'].values,box_date)
        except:
            print(lno(),df)
            raise
        #print(lno(),stokc_id,box_high,box_low,long_ratio)
        #long_ratio,box_high,box_low=self.check_long_candle(df)
        #print(lno(),stokc_id,box_high,box_low,long_ratio)
        if long_ratio < 3 :
            return
        self.out.append([self.rundate,self.market,r.stock_id,'%s'%(self.strategy),box_high,box_low,long_ratio]) 
    def get_longred_sql(self,startdate,enddate):
        table_name='verylongred_v1'
        cmd='SELECT * FROM "{}" WHERE date >= "{}" and date < "{}"'.format(table_name,startdate,enddate+relativedelta(days=1))
        df = pd.read_sql(cmd, con=self.con, parse_dates=['date']) 
        return df
    def test1(self,r):
        if comm.check_stock_id(r.stock_id)==False:
            return 
        #print(lno(),r)
        if float(r.vol)<1000:
            return
        if type( r['diff'])==str and '--' in r['diff']:
            return
        try:
            diff=float(r['diff'])
            if diff<=0:
                return
        except:
            print(lno(),type(r['diff']),r['diff'])
            raise     
        if len(self.cmp_df)==0:
            enddate=datetime.strptime('20091218', '%Y%m%d')+relativedelta(days=1)
            startdate=enddate-relativedelta(months=5)
            self.cmp_df=self.stk.get_df_by_startdate_enddate('3026',startdate,enddate).tail(60).reset_index(drop=True)
            #print(lno(),self.cmp_df)
        box_date=60
        startdate=self.rundate-relativedelta(months=5)
        ##need rundate data 
        enddate=self.rundate+relativedelta(days=1)
            
        df_all=self.stk.get_df_by_startdate_enddate(r.stock_id,startdate,enddate)

        if len(df_all)<60:
            return
        #df_all=comm.get_stock_df(stokc_id)
        #print(lno(),df_all.open.values)
        df=df_all.tail(60)
        ## 3026  2009 12 18 前60  
        #https://kknews.cc/zh-tw/invest/x6q942o.html
        corropen=round(np.corrcoef(self.cmp_df.open,df.open)[0][1],3)
        corrohigh=round(np.corrcoef(self.cmp_df.high,df.high)[0][1],3)
        corrlow=round(np.corrcoef(self.cmp_df.low,df.low)[0][1],3)
        corrclose=round(np.corrcoef(self.cmp_df.close,df.close)[0][1],3)
        T=(corropen+corrohigh+corrlow+corrclose)/4
        T=round(T,2)
        #print(lno(),r.stock_id,T)
        #print(lno(),stokc_id,box_high,box_low,long_ratio)
        #long_ratio,box_high,box_low=self.check_long_candle(df)
        #print(lno(),stokc_id,box_high,box_low,long_ratio)
        if (T>=0.8): 
            print(lno(),r.stock_id,T)
            self.out.append([self.rundate,self.market,r.stock_id,'%s'%(self.strategy),T])    
    
    def run(self,rundate,strategy):
        self.rundate=rundate
        self.strategy=strategy
        if '10' in self.strategy:
            self.boxdate=10
        elif '20' in self.strategy:    
            self.boxdate=20
        else:
            self.boxdate=60
        markets=['tse','otc']
        tStart = time.time()
        self.out=[]
        for market in markets:
            if market=='tse':
                self.market='tse'
                d=self.tse.get_df(self.rundate)
            else:
                self.market='otc'
                d=self.otc.get_df(self.rundate)
            
                #print(lno(),out)
            if self.strategy=='VeryLongRed_10day':    
                d.apply(self.find_stong_red_stk,axis=1)    
            elif  self.strategy=='test1':     
                d.apply(self.test1,axis=1)    
            else:
                raise    
        if self.strategy=='VeryLongRed_10day':           
            columns=['date','market','stock_id','買進信號','box_h','box_l','實體K_ratio']
        elif  self.strategy=='test1':     
            columns=['date','market','stock_id','買進信號','相似度']
        df=pd.DataFrame(self.out, columns=columns)
        if len(df):
            engine = create_engine('sqlite:///sql/buy_signal.db', echo=False)
            con = engine.connect()
            enddate= self.rundate + relativedelta(days=1)    
            #print(lno(),date_str,df['stock_id'].values.tolist())
            date_str=self.rundate.strftime('%Y%m%d')
            table_names = self.engine.table_names() 
            if self.strategy in table_names:
                cmd='SELECT * FROM "{}" WHERE date >= "{}" and date < "{}"'.format(self.strategy,self.rundate,enddate)
                df_query= pd.read_sql(cmd, con=self.con)
                if len(df_query.index)==0:  
                    print(lno(),date_str)      
                    df.to_sql(name=self.strategy, con=self.con, if_exists='append', index=False,chunksize=10)
                    #raise
                else :
                    #print(lno(),df_query)   
                    pass 
            else:
                df.to_sql(name=self.strategy, con=self.con, if_exists='append', index=False,chunksize=10)    
            #print(lno(),date_str)  
            #df1 = pd.read_sql('select * from "{}"'.format(date_str), con=con)  
            #print(lno(),df1)  
        tEnd = time.time()
        print ("It cost %.3f sec" % (tEnd - tStart))   
    """
    1.集中：短、中、長期籌碼集中
    2.糾結：均線糾結、準備翻揚向上
    3.壓縮：過去一段時間價穩、量縮
    4.突破：突破切線、頸線或是前波高點
    5.帶量：有量增，但不是爆量
    6.實紅：實體長紅，上影線長度不超過實體 1/2
    1. 周K線 三線是費氏數列的 8MA、21MA、55MA   也可以使用常見的5MA 20MA 60MA 來判斷
    2.指標：MACD位於零軸附近  MACD只要位於零軸附近  代表著均線很接近 可以快速判斷出均線糾結 和糾結的時間長度
    3.線圖：直覺右下角  拉出 4-5 年的周線圖 劃出一條中間線  只要K棒位處於中間線的右下角  就能大膽的判斷出位階處於低位！
    """
    ## 均線糾結 https://blog.csdn.net/luoganttcc/article/details/80159025
    ## part2 https://xstrader.net/%E6%95%A3%E6%88%B6%E7%9A%8450%E9%81%93%E9%9B%A3%E9%A1%8C%E7%8B%97%E5%B0%BE%E7%89%88%E4%B9%8B4%E5%A6%82%E4%BD%95%E5%88%A4%E6%96%B7%E4%B8%80%E6%AA%94%E8%82%A1%E7%A5%A8%E5%8F%AF%E4%BB%A5%E9%95%B7/
    
    ##TODO Q指標 https://xstrader.net/%E7%A8%8B%E5%BC%8F%E4%BA%A4%E6%98%93%E5%9C%A8%E5%9F%BA%E9%87%91%E6%8A%95%E8%B3%87%E4%B8%8A%E7%9A%84%E6%87%89%E7%94%A8%E4%B9%8B%E4%BA%8Cq%E6%8C%87%E6%A8%99/  
    def analy(self,method,startdate,enddate):
        tStart = time.time()
        table_name=method
        cmd='SELECT * FROM "{}" WHERE date >= "{}" and date <= "{}"'.format(table_name,startdate,enddate)
        df1 = pd.read_sql(cmd, con=self.con, parse_dates=['date'])  
        tEnd = time.time()
        print ("It cost %.3f sec" % (tEnd - tStart))
        #print(lno(),df1) 
        tStart = time.time()
    
        def get_price(r):
            date=r.date
            stock_id=r.stock_id
            #if r.stock_id!='6130':
            #    return
            total_stock_nums=self.tdcc.get_total_stock_num(r.stock_id,r.date)
            # 市值 >=100億 大
            # 高低位,get 3 year 
            #print(lno(),stock_id,date)
            #raise
            ## 突破當日資料不要
            df1=self.stk.get_df_by_enddate_num(stock_id,date-relativedelta(days=1),120)
            ma_list = [5,21,89]
            for ma in ma_list:
                df1['MA_' + str(ma)] = df1['close'].rolling(window=ma,center=False,axis=0).mean()
            #print(lno(),df1)

            ma5_angle=np.nan    
            ma21_angle=np.nan    
            ma89_angle=np.nan    
            datanum=len(df1.index)    
            try:
                if datanum>=6:
                    ma5_angle=talib.LINEARREG_ANGLE(df1['MA_5'].values,2)[-1]    
                if datanum>=22:
                    ma21_angle=talib.LINEARREG_ANGLE(df1['MA_21'].values,2)[-1]    
                if datanum>=90:
                    ma89_angle=talib.LINEARREG_ANGLE(df1['MA_89'].values,2)[-1]   
            except:
                print(lno(),stock_id)
                print(lno(),df1.close)

            #print(lno(),ma5_angle,ma21_angle,ma89_angle)
            #raise
            edate= date + relativedelta(days=100)  
            df0=self.stk.get_df_by_startdate_enddate(stock_id,date,edate )
            #print(lno(),df0.close)
            buy=np.nan
            day5=np.nan
            day20=np.nan
            day60=np.nan
            datanum=len(df0.index)
            if datanum==0:
                return
            if datanum>=2:
                buy=df0.at[1,'open']
            if datanum>=6 and buy!=0:
                sell=df0.at[5,'close']
                if sell!=0:
                    day5=cm1.calc_profit(buy,sell) 
            if datanum>=21 and buy!=0:
                sell=df0.at[20,'close']
                if sell!=0:
                    day20=cm1.calc_profit(buy,sell) 
            if datanum>=61 and buy!=0 :
                sell=df0.at[60,'close']
                if sell!=0:
                    day60=cm1.calc_profit(buy,sell) 
                #print(lno(),df0)
                #print(lno(),stock_id,date,enddate)
                #raise  
            if datanum>=1:
                value=int(total_stock_nums*df0.at[0,'close']/100000000)
            else:
                value=np.nan    
            #print(lno(),day5,day20,day60,value,ma5_angle,ma21_angle,ma89_angle)    
            return day5,day20,day60,value,ma5_angle,ma21_angle,ma89_angle
        df1[['day5','day20','day60','市值(億)','MA5(角度)','MA21(角度)','MA89(角度)']]=df1.apply(get_price,axis=1,result_type="expand")
        df1.to_sql(name='verylongred_v1', con=self.con, if_exists='replace', index=False,chunksize=10)
        print(lno(),df1)
        comm.to_html(df1,'out/buy_signal/{}_{}-{}.html'.format(table_name,startdate.strftime('%Y%m%d'),enddate.strftime('%Y%m%d')))
        cnt=len(df1.index)
        d5_num=cnt-df1['day5'].isna().sum()
        d20_num=cnt-df1['day20'].isna().sum()
        d60_num=cnt-df1['day60'].isna().sum()
        d5_ok=len(df1.loc[df1['day5'] >= 1])
        d20_ok=len(df1.loc[df1['day20'] >= 1])
        d60_ok=len(df1.loc[df1['day60'] >= 1])
        d5_porfit=df1['day5'].sum()
        d20_porfit=df1['day20'].sum()
        d60_porfit=df1['day60'].sum()

        _list=[d5_ok/d5_num,d5_porfit/d5_num,d20_ok/d20_num,d20_porfit/d20_num,d60_ok/d60_num,d60_porfit/d60_num]
        print(lno(),_list)
        cols=['5日勝率','5日獲利','20日勝率','20日獲利','60日勝率','60日獲利']
        dfo=pd.DataFrame([_list],columns=cols).round({'5日勝率': 2,'5日獲利': 5,'20日勝率': 2,'20日獲利': 5,'60日勝率': 2,'60日獲利': 5})

        comm.to_html(dfo,'out/buy_signal/{}_{}-{}fin.html'.format(table_name,startdate.strftime('%Y%m%d'),enddate.strftime('%Y%m%d') ))
        print(lno(),dfo)
        #print(d5_num,d5_ok,d5_porfit/d5_num)
        #print(d20_num,d20_ok,d20_porfit/d20_num)
        #print(d60_num,d60_ok,d60_porfit/d60_num)
        
        tEnd = time.time()
        print ("It cost %.3f sec" % (tEnd - tStart))           

        ## TODO  k線型態 辨識 突起物 
        # 自動偵測高低點
        #threshold = 750 # 設定突起度門檻
        #peaks = signal.find_peaks(stock['price'], prominence=threshold)[0] # 找相對高點
        #valleys = signal.find_peaks(-stock['price'], prominence=threshold)[0]
        #df1=comm.get_stock_df_bydate_nums(i,240,self.rundate)
        #df1=comm.get_stock_df(i)
        """
        ## 3026  2009 12 21 前60  
        #https://kknews.cc/zh-tw/invest/x6q942o.html
        corropen=round(np.corrcoef(open[s1],open[s2])[0][1],3)
        corrohigh=round(np.corrcoef(high[s1],high[s2])[0][1],3)
        corrlow=round(np.corrcoef(low[s1],low[s2])[0][1],3)
        corrclose=round(np.corrcoef(close[s1],close[s2])[0][1],3)
        T=(corropen+corrohigh+corrlow+corrclose)/4
        T=round(T,2)
        #https://www.itread01.com/content/1547222045.html
        #https://ufclark.pixnet.net/blog/post/3844923
        """
        
    def run_fun(self,rundate,fun,table_name):
        self.rundate=rundate
        markets=['tse','otc']
        tStart = time.time()
        d1=self.tse.get_df_date_parse(self.rundate)
        d1['market']='tse'
        d2=self.otc.get_df_date_parse(self.rundate)
        d2['market']='otc'
        d=pd.concat([d1,d2])
        if len(d.index)==0:
            return 
        d['result']=d.apply(fun,axis=1)  
        df=d.dropna()[['date','market','stock_id','result']]  
        print(lno(), df)  
        if len(df):
            #print(lno(),date_str,df['stock_id'].values.tolist())
            table_names = self.engine.table_names() 
            if table_name in table_names:
                cmd='SELECT * FROM "{}" WHERE date >= "{}" and date < "{}"'.format(table_name,rundate,rundate+relativedelta(days=1)  )
                df_query= pd.read_sql(cmd, con=self.con)
                if len(df_query.index)==0:  
                    print(lno(),rundate)      
                    df.to_sql(name=table_name, con=self.con, if_exists='append', index=False,chunksize=10)
                    #raise
                else :
                    #print(lno(),df_query)   
                    pass 
            else:
                df.to_sql(name=table_name, con=self.con, if_exists='append', index=False,chunksize=10)    
        tEnd = time.time()
        print ("It cost %.3f sec" % (tEnd - tStart))   
    
from sqlalchemy import create_engine       
from sqlalchemy.types import NVARCHAR, Float, Integer 
def gen_final_df(df_fin,name,df1):
    cnt=len(df1.index)
    d5_num=cnt-df1['day5'].isna().sum()
    d20_num=cnt-df1['day20'].isna().sum()
    d60_num=cnt-df1['day60'].isna().sum()
    d5_ok=len(df1.loc[df1['day5'] >= 1])
    d20_ok=len(df1.loc[df1['day20'] >= 1])
    d60_ok=len(df1.loc[df1['day60'] >= 1])
    d5_porfit=df1['day5'].sum()
    d20_porfit=df1['day20'].sum()
    d60_porfit=df1['day60'].sum()
    _list=[name,d5_num,d20_num,d60_num,d5_ok/d5_num,d5_porfit/d5_num,d20_ok/d20_num,d20_porfit/d20_num,d60_ok/d60_num,d60_porfit/d60_num]
    print(lno(),d5_num,d20_num,d60_num)
    cols=['名稱','5日總數','20日總數','60日總數','5日勝率','5日獲利','20日勝率','20日獲利','60日勝率','60日獲利']
    dfo=pd.DataFrame([_list],columns=cols).round({'5日勝率': 2,'5日獲利': 5,'20日勝率': 2,'20日獲利': 5,'60日勝率': 2,'60日獲利': 5})
    df_fin=pd.concat([df_fin, dfo])
    return df_fin
    #return _list
def gen_cond_fin_df(df_fin,cond,df):
    cond0=cond[0][0]
    va0=cond[0][1]
    
    if len(cond)==1: ##單條件
        if len(cond[0])==4:
            cond1=cond[0][0]
            va1=cond[0][1]    
            va2=cond[0][2]
            va3=cond[0][3]
            str='{}>={}'.format(cond1,va1)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond1] >=va1 ) ] )
            str='{}<{} >={}'.format(cond1,va1,va2)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond1] <va1 )&(df.loc[:,cond1] >=va2 ) ] )
            str='{}<{} >={}'.format(cond1,va2,va3)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond1] <va2 )&(df.loc[:,cond1] >=va3 ) ] )
            str='{}<{} '.format(cond1,va3)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond1] <va3 ) ] ) 
        else:    
            str='{}>={}'.format(cond0,va0)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond0] >= va0) ] )
            str='{}<{}'.format(cond0,va0)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond0] < va0)  ] )
    elif len(cond)==2:
        cond1=cond[1][0]
        va1=cond[1][1]    
        if len(cond[1])==4: ##第2條件 有3項
            va2=cond[1][2]
            va3=cond[1][3]
            str='{}>={} {}>={}'.format(cond0,va0,cond1,va1)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond0] >= va0) & (df.loc[:,cond1] >=va1 ) ] )
            str='{}>={} {}<{} >={}'.format(cond0,va0,cond1,va1,va2)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond0] >= va0) & (df.loc[:,cond1] <va1 )&(df.loc[:,cond1] >=va2 ) ] )
            str='{}>={} {}<{} >={}'.format(cond0,va0,cond1,va2,va3)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond0] >= va0) & (df.loc[:,cond1] <va2 )&(df.loc[:,cond1] >=va3 ) ] )
            str='{}>={} {}<{} '.format(cond0,va0,cond1,va3)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond0] >= va0) & (df.loc[:,cond1] <va3 ) ] ) 

            str='{}<{} {}>={}'.format(cond0,va0,cond1,va1)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond0] < va0) & (df.loc[:,cond1] >=va1 ) ] )
            str='{}<{} {}<{} >={}'.format(cond0,va0,cond1,va1,va2)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond0] < va0) & (df.loc[:,cond1] <va1 )&(df.loc[:,cond1] >=va2 ) ] )
            str='{}<{} {}<{} >={}'.format(cond0,va0,cond1,va2,va3)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond0] < va0) & (df.loc[:,cond1] <va2 )&(df.loc[:,cond1] >=va3 ) ] )
            str='{}<{} {}<{} '.format(cond0,va0,cond1,va3)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond0] < va0) & (df.loc[:,cond1] <va3 ) ] ) 

        else:    
            str='{}>={} {}>={}'.format(cond0,va0,cond1,va1)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond0] >= va0) & (df.loc[:,cond1] >=va1 ) ] )
            str='{}>={} {}<{}'.format(cond0,va0,cond1,va1)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond0] >= va0) & (df.loc[:,cond1] <va1 ) ] )
            str='{}<{} {}>={}'.format(cond0,va0,cond1,va1)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond0] < va0) & (df.loc[:,cond1] >=va1 ) ] )
            str='{}<{} {}<{}'.format(cond0,va0,cond1,va1)
            df_fin=gen_final_df(df_fin,str,df[(df.loc[:,cond0] < va0) & (df.loc[:,cond1] <va1 ) ] )
    return df_fin

def find_stock_analy(method,startdate,enddate):
    engine = create_engine('sqlite:///sql/buy_signal.db', echo=False)
    stk=get_stock_data
    con = engine.connect() 

    table_name=method
    cmd='SELECT * FROM "{}" WHERE date >= "{}" and date <= "{}"'.format(table_name,startdate,enddate)
    df_all = pd.read_sql(cmd, con=con, parse_dates=['date']) 
    
    df_all = df_all.rename({'ma_tangled_day': '均線糾纏日期', 'upper_shadow': '上影線比例', 'over_prev_high': '過昨日高', 'mv5_vol_ratio': '量增比例'}, axis=1)
    df_out=pd.DataFrame()
    list=['市值>=100億','市值<100億']
    for i in list:
        if '>' in i :
            df=df_all[(df_all.loc[:,"市值(億)"] >= 100)]
            filen='big'
        else:
            df=df_all[(df_all.loc[:,"市值(億)"] < 100)]
            filen='small'
        df_fin=pd.DataFrame()      
        df_fin=gen_final_df(df_fin,i,df)
        cond=[["MA89(角度)",0.0]]
        df_fin=gen_cond_fin_df(df_fin,cond,df)
        cond=[["MA21(角度)",0.0]]
        df_fin=gen_cond_fin_df(df_fin,cond,df)
        cond=[["MA5(角度)",0.0]]
        df_fin=gen_cond_fin_df(df_fin,cond,df)
        cond=[["ma1_ma5",1]]
        df_fin=gen_cond_fin_df(df_fin,cond,df)
        cond=[["ma1_ma21",1]]
        df_fin=gen_cond_fin_df(df_fin,cond,df)
        cond=[["ma5_ma21",1]]
        df_fin=gen_cond_fin_df(df_fin,cond,df)
        if 'tdcc' in table_name:
            cond=[["大戶買超",500]]
            df_fin=gen_cond_fin_df(df_fin,cond,df)
            cond=[["中戶買超",500]] 
            df_fin=gen_cond_fin_df(df_fin,cond,df)
        cond=[["投信",200000]]
        df_fin=gen_cond_fin_df(df_fin,cond,df)
        cond=[["外資",200000]]
        df_fin=gen_cond_fin_df(df_fin,cond,df)
        cond=[['量增比例',5,2,1]]
        df_fin=gen_cond_fin_df(df_fin,cond,df)
        
        cond=[["月營收YoY",20]]
        df_fin=gen_cond_fin_df(df_fin,cond,df)
        cond=[["月營收均線法",1]]
        df_fin=gen_cond_fin_df(df_fin,cond,df)
        cond=[["月營收成長法",1]]
        df_fin=gen_cond_fin_df(df_fin,cond,df)
        
        cond=[["均線糾纏日期",5]]
        df_fin=gen_cond_fin_df(df_fin,cond,df)
        cond=[["上影線比例",0.3]]
        df_fin=gen_cond_fin_df(df_fin,cond,df)
        cond=[["過昨日高",0.0]]
        df_fin=gen_cond_fin_df(df_fin,cond,df)
        cmp= df_fin.iloc[0]['5日勝率']
        df_fin['5日勝率'] = df_fin['5日勝率'].apply(lambda x: f'<font color="red">{x}</font>'.format(x) if x>cmp else x)
        cmp= df_fin.iloc[0]['5日獲利']
        df_fin['5日獲利'] = df_fin['5日獲利'].apply(lambda x: f'<font color="red">{x}</font>'.format(x) if x>cmp else x)
        
        cmp= df_fin.iloc[0]['20日勝率']
        df_fin['20日勝率'] = df_fin['20日勝率'].apply(lambda x: f'<font color="red">{x}</font>'.format(x) if x>cmp else x)
        cmp= df_fin.iloc[0]['20日獲利']
        df_fin['20日獲利'] = df_fin['20日獲利'].apply(lambda x: f'<font color="red">{x}</font>'.format(x) if x>cmp else x)
        
        cmp= df_fin.iloc[0]['60日勝率']
        df_fin['60日勝率'] = df_fin['60日勝率'].apply(lambda x: f'<font color="red">{x}</font>'.format(x) if x>cmp else x)
        cmp= df_fin.iloc[0]['60日獲利']
        df_fin['60日獲利'] = df_fin['60日獲利'].apply(lambda x: f'<font color="red">{x}</font>'.format(x) if x>cmp else x)
        comm.to_html(df_fin,'out/buy_signal/{}-{}_{}-{}fin.html'.format(table_name,filen,startdate.strftime('%Y%m%d'),enddate.strftime('%Y%m%d') ))
        df_out=pd.concat([df_out,df_fin])
    """
    cond=[["市值(億)",100],["MA89(角度)",0.0]]
    df_fin=gen_cond_fin_df(df_fin,cond,df)
    cond=[["市值(億)",100],["MA21(角度)",0.0]]
    df_fin=gen_cond_fin_df(df_fin,cond,df)
    
    cond=[["市值(億)",100],["均線糾纏日期",5]]
    df_fin=gen_cond_fin_df(df_fin,cond,df)
    cond=[["市值(億)",100],["上影線比例",0.3]]
    df_fin=gen_cond_fin_df(df_fin,cond,df)
    cond=[["市值(億)",100],["過昨日高",0.0]]
    df_fin=gen_cond_fin_df(df_fin,cond,df)
    cond=[["市值(億)",100],['量增比例',5,2,1]]
    df_fin=gen_cond_fin_df(df_fin,cond,df)
    cond=[["市值(億)",100],["外資",200000]]
    df_fin=gen_cond_fin_df(df_fin,cond,df)
    cond=[["市值(億)",100],["投信",200000]]
    df_fin=gen_cond_fin_df(df_fin,cond,df)
    
    print(lno(),df_fin)
    cmp= df_fin.iloc[0]['5日勝率']
    df_fin['5日勝率'] = df_fin['5日勝率'].apply(lambda x: f'<font color="red">{x}</font>'.format(x) if x>cmp else x)
    cmp= df_fin.iloc[0]['5日獲利']
    df_fin['5日獲利'] = df_fin['5日獲利'].apply(lambda x: f'<font color="red">{x}</font>'.format(x) if x>cmp else x)
    
    cmp= df_fin.iloc[0]['20日勝率']
    df_fin['20日勝率'] = df_fin['20日勝率'].apply(lambda x: f'<font color="red">{x}</font>'.format(x) if x>cmp else x)
    cmp= df_fin.iloc[0]['20日獲利']
    df_fin['20日獲利'] = df_fin['20日獲利'].apply(lambda x: f'<font color="red">{x}</font>'.format(x) if x>cmp else x)
    
    cmp= df_fin.iloc[0]['60日勝率']
    df_fin['60日勝率'] = df_fin['60日勝率'].apply(lambda x: f'<font color="red">{x}</font>'.format(x) if x>cmp else x)
    cmp= df_fin.iloc[0]['60日獲利']
    df_fin['60日獲利'] = df_fin['60日獲利'].apply(lambda x: f'<font color="red">{x}</font>'.format(x) if x>cmp else x)
    """
    print(lno(),df_out)
    comm.to_html(df_out,'out/buy_signal/{}_{}-{}fin.html'.format(table_name,startdate.strftime('%Y%m%d'),enddate.strftime('%Y%m%d') ))
 
def find_ma1_cross_ma5(df):
    if len(df.index)==0:
        return np.nan
    print(lno(),df)
    ma_list = [5,10,20]
    #tmp=[]
    for ma in ma_list:
        df['MA_' + str(ma)] = talib.MA(df.close,ma)
        #tmp.append(talib.LINEARREG_ANGLE(df1['MA_'+ str(ma)].values,ma)[-1])
    #print(lno(),df.tail(5))
    threshold = df.iloc[-1]['close']*0.025
    print(lno(),threshold)
    peaks, _ = signal.find_peaks(df.close,prominence=threshold, distance=2)
    print(lno(),peaks)
    prominences = signal.peak_prominences(df.close, peaks)[0]
    print(lno(),prominences)
    #peaks = signal.find_peaks(df.close, prominence=threshold) # 找相對高點 
    #print(lno(),peaks[0])
    kline.show_stock_kline_pic_by_df(df)
    sys.exit()   
    return 1
    pass
def red_K_ratio_calc(r):
    try:
        pre_close=r.close-r['diff']
        if r.open >pre_close:
            red_K_pwr= (r.close -pre_close)/pre_close
        else :
            red_K_pwr= (r.close -r.open)/pre_close    
        return red_K_pwr 
    except :
        return np.nan
def Kbody_calc(r):
    try:
        return abs(r.close-r.open)
    except :
        return np.nan    
def find_point_K(r):
    try :
        cash=float(r.cash)
        if cash<3000000:
            return 
        open=float(r.open)
        close=float(r.close)
        if open>=close:
            return
        diff=float(r['diff'])
        pre_close=close-diff
        if open >pre_close:
            red_K_ratio= (close -pre_close)/pre_close
        else :
            red_K_ratio= (close -open)/pre_close
        if red_K_ratio<=0.03:
            return 
    except:
        print(lno(),r)    
        if type(r['diff'])==str and '除' in r['diff'] :
            return
        if type(r.open)==str and '--' in r.open:
            return
        
        raise
    #print(lno(),red_K_ratio,r.stock_id,r.date)
    stk=get_stock_data()
    df1=stk.get_df_by_enddate_num(r.stock_id,r.date,122)
    #print(lno(),df1)
    if len(df1)<90:
        return
    df1['red_K_ratio']=df1.apply(red_K_ratio_calc, axis=1)
    top10=df1.sort_values(by='red_K_ratio',ascending=False).iloc[10]['red_K_ratio']
    #print(lno(),df1,top10)
    if df1.iloc[-1]['red_K_ratio']>=top10:
        return 1
    return 

def find_fund_buy(r):
    try :
        cash=float(r.cash)
        if cash<3000000:
            return
        if len(r.stock_id)!=4:
            return 
        
        tdcc=get_tdcc_dist() 
        total_stock_nums=tdcc.get_total_stock_num(r.stock_id,r.date)
        if total_stock_nums==0:
            return 
        sb3=get_stock_big3()
        df=sb3.get_df_by_id_date(r.stock_id,r.date)
        #print(lno(),df)
        if len(df.index)==0:
            return 
        else:    
            #out=df[['外陸資買賣超股數(不含外資自營商)','投信買賣超股數','自營商買賣超股數','三大法人買賣超股數']].values[0].tolist()
            fund_buy=df.iloc[0]['投信買賣超股數']
            #print(lno(),fund_buy)
            if fund_buy>200*1000:
                return fund_buy
            return 
    except:
        print(lno())
        return 
   
##TODO: find_point_K  傳function 給run_fun 做股票篩選
def findstock_run_func(startdate,enddate,func):
    fs=findstock()
    nowdate=enddate
    day=0
    while   nowdate>=startdate :
        nowdate = enddate - relativedelta(days=day)
        print(lno(),nowdate)
        fs.run_fun(nowdate, func,func.__name__)
        day=day+1 


multitask=0
def df_apply_fun(df,func):
    global multitask
    if platform.system().upper()=='LINUX': 
        multitask=1   
        pandarallel.initialize()
        return df.parallel_apply(func,axis=1)
    else:
        multitask=0   
        return df.apply(func, axis=1)      
def pool_map(function_name, df, processes = multiprocessing.cpu_count()):
    if platform.system().upper()=='LINUX':
        n_processes = processes  # My machine has 4 CPUs
    else:
        n_processes = 1
    df_split = np.array_split(df, n_processes)
    return multiprocessing.Pool(processes).map(function_name, df_split)
g_stk = None
g_engine=None
g_con=None
def init_sql():
    global g_stk,g_engine,g_con
    if g_stk==None:
        print(lno())
        g_stk=comm.stock_data()
    if g_engine==None:
        g_engine = create_engine('sqlite:///sql/buy_signal.db', echo=False)  
    if g_con==None: 
        g_con = g_engine.connect()    
    return   g_stk,g_engine,g_con
 
def get_stock_data():
    global g_stk
    if multitask==1:
        stk=comm.stock_data()
    else:
        if g_stk==None:
            print(lno(),'111')
            g_stk=comm.stock_data()
        stk=g_stk
    return stk

g_tdcc=None
def get_tdcc_dist():
    global g_tdcc
    if multitask==1:
        tdcc=tdcc_dist.tdcc_dist()
        return tdcc
    else:
        if g_tdcc==None:
            g_tdcc=tdcc_dist.tdcc_dist()
        return g_tdcc 
g_sb3=None
def get_stock_big3():
    global g_sb3
    if multitask==1:
        sb3=stock_big3.stock_big3()
        return sb3
    else:
        if g_sb3==None:
            g_sb3=stock_big3.stock_big3()
            print(lno(),g_sb3)    
        return g_sb3 
g_income=None
def get_sql_income():
    global g_income
    if multitask==1:
        in1=revenue.income()
        return in1
    else:
        if g_income==None:
            g_income=revenue.income()
            print(lno(),g_income)    
        return g_income 
def mv5_vol_ratio(r):
    stk=get_stock_data()
    df1=stk.get_df_by_enddate_num(r.stock_id,r.date,20)
    try :
        df1['MV5'] = talib.MA(df1.vol,5)
        v_ratio=df1.iloc[-1]['vol']/df1.iloc[-2]['MV5']
    except:
        print(lno(),r.stock_id,r.date,"some thing wrong",df1)
        #raise
        return np.nan     
    return v_ratio
def upper_shadow(r):
    stk=get_stock_data()   
    df1=stk.get_df_by_enddate_num(r.stock_id,r.date,1)
    #print(lno(),r.date,df1)
    try:
        upper_shadow_val=df1.at[0,'high']-df1.at[0,'close']
        real_body=abs(df1.at[0,'close']-df1.at[0,'open'])
        if real_body==0:
            return np.nan
        return upper_shadow_val/real_body
    except:
        return np.nan    
def over_prev_high(r):
    stk=get_stock_data() 
    df1=stk.get_df_by_enddate_num(r.stock_id,r.date,20)
    #print(lno(),r.date,df1)
    try :
        over_phigh=df1.iloc[-1]['close']-df1.iloc[-2]['high']
    except:
        print(lno(),"some thing wrong",df1)
        return np.nan     
    return over_phigh
def ma_tangled_day(r):
    stk=get_stock_data() 
    df1=stk.get_df_by_enddate_num(r.stock_id,r.date-relativedelta(days=1),120)
    try :
        ma_list = [5,10,20]
        for ma in ma_list:
            df1['MA_' + str(ma)] = talib.MA(df1.close,ma)
    except:
        print(lno(),r.stock_id,r.date,len(df1.index))  
        if len(df1.index)==0:
            return np.nan      
        raise
    def calc_sizeway(r):
        max_value=max(r.MA_5,r.MA_10,r.MA_20)
        min_value=min(r.MA_5,r.MA_10,r.MA_20)
        if min_value==0:
            return np.nan
        return (max_value-min_value)/min_value*100
    sideway=df1.apply(calc_sizeway,axis=1)
    rev_sideway=sideway[::-1]
    cnt=0
    for i in rev_sideway:
        if i<=0.5:
            cnt=cnt+1
            continue
        break
    if abs(cnt)>=5 :
        #print(lno(),r.stock_id,r.date,tmp)    
        #kline.show_stock_kline_pic(r.stock_id,r.date,120)
        pass
    return cnt

def get_analy_1(r):
    buy=np.nan
    day5=np.nan
    day20=np.nan
    day60=np.nan
    ma1_ma5=np.nan    
    ma1_ma21=np.nan    
    ma5_ma21=np.nan    
    ma5_angle=np.nan    
    ma21_angle=np.nan    
    ma89_angle=np.nan
    value=np.nan      
    date=r.date
    stock_id=r.stock_id
    
    if len(stock_id)!=4:
        return day5,day20,day60,value,ma5_angle,ma21_angle,ma89_angle,ma1_ma5,ma1_ma21,ma5_ma21
    stk=get_stock_data()
    tdcc=get_tdcc_dist() 
    total_stock_nums=tdcc.get_total_stock_num(r.stock_id,r.date)
    df1=stk.get_df_by_enddate_num(stock_id,date-relativedelta(days=1),120)
    ma_list = [5,21,89]
    try:
        for ma in ma_list:
            df1['MA_' + str(ma)] = df1['close'].rolling(window=ma,center=False,axis=0).mean()
    except:
        print(lno(),df1)
        if len(df1.index)==0:
            return day5,day20,day60,value,ma5_angle,ma21_angle,ma89_angle,ma1_ma5,ma1_ma21,ma5_ma21 
    
    datanum=len(df1.index)    
    try:
        if datanum>=6:
            ma1_ma5=df1.iloc[-1]['close'] /df1.iloc[-1]['MA_5']
            ma5_angle=talib.LINEARREG_ANGLE(df1['MA_5'].values,2)[-1]    
        if datanum>=22:
            ma1_ma21=df1.iloc[-1]['close'] /df1.iloc[-1]['MA_21']
            ma5_ma21=df1.iloc[-1]['MA_5'] /df1.iloc[-1]['MA_21']
            ma21_angle=talib.LINEARREG_ANGLE(df1['MA_21'].values,2)[-1]    
        if datanum>=90:
            ma89_angle=talib.LINEARREG_ANGLE(df1['MA_89'].values,2)[-1]   
    except:
        print(lno(),stock_id)
        print(lno(),df1.close)

    #print(lno(),ma5_angle,ma21_angle,ma89_angle)
    #raise
    edate= date + relativedelta(days=100)  
    df0=stk.get_df_by_startdate_enddate(stock_id,date,edate )
    #print(lno(),df0.close)
    
    datanum=len(df0.index)
    if datanum==0:
        return day5,day20,day60,value,ma5_angle,ma21_angle,ma89_angle,ma1_ma5,ma1_ma21,ma5_ma21
    if datanum>=2:
        buy=df0.at[1,'open']
    if datanum>=6 and buy!=0:
        sell=df0.at[5,'close']
        if sell!=0:
            day5=cm1.calc_profit(buy,sell) 
    if datanum>=21 and buy!=0:
        sell=df0.at[20,'close']
        if sell!=0:
            day20=cm1.calc_profit(buy,sell) 
    if datanum>=61 and buy!=0 :
        sell=df0.at[60,'close']
        if sell!=0:
            day60=cm1.calc_profit(buy,sell) 
    if datanum>=1:
        value=int(total_stock_nums*df0.at[0,'close']/100000000)
       
    #print(lno(),ma1_ma5,ma1_ma21,ma5_ma21)    
    return day5,day20,day60,value,ma5_angle,ma21_angle,ma89_angle,ma1_ma5,ma1_ma21,ma5_ma21

def get_ma5_21(r):
    date=r.date
    stock_id=r.stock_id
    stk=get_stock_data()
    df1=stk.get_df_by_enddate_num(stock_id,date-relativedelta(days=1),120)
    ma_list = [5,21]
    for ma in ma_list:
        df1['MA_' + str(ma)] = df1['close'].rolling(window=ma,center=False,axis=0).mean()

    ma1_ma5=np.nan    
    ma1_ma21=np.nan    
    ma5_ma21=np.nan    
  
    datanum=len(df1.index)    
    try:
        if datanum>=6:
            ma1_ma5=df1.iloc[-1]['close'] /df1.iloc[-1]['MA_5']
        if datanum>=22:
            ma1_ma21=df1.iloc[-1]['close'] /df1.iloc[-1]['MA_21']
            ma5_ma21=df1.iloc[-1]['MA_5'] /df1.iloc[-1]['MA_21']

    except:
        print(lno(),stock_id)
        print(lno(),df1.close)

    return ma1_ma5,ma1_ma21,ma5_ma21

##投信買超
def get_big3_buy_vol(r):
    #print(lno(),r.stock_id,r.date)
    sb3=get_stock_big3()
    df=sb3.get_df_by_id_date(r.stock_id,r.date)
    #print(lno(),df)
    if len(df.index)==0:
        out=[0,0,0,0]
    else:    
        out=df[['外陸資買賣超股數(不含外資自營商)','投信買賣超股數','自營商買賣超股數','三大法人買賣超股數']].values[0].tolist()
    #print(lno(),out)
    #sys.exit()   
    return out
def get_tdcc_dist_1000_400_buy_vol(r):
    tdcc=get_tdcc_dist() 
    b_diff,m_diff=tdcc.get_tdcc_1000_400(r.stock_id,r.date)
    return b_diff,m_diff
def get_income_ratio(r):
    YOY=np.nan
    ratio1=np.nan
    result=np.nan
    income=get_sql_income()
    date=r.date
    ##TODO：
    """
    均線法: 當近三個月營收平均 > 近一年月營收平均   
    突破法:24個月創新高最有用！ 
    MOM 這個月跟上個月比較
    QOQ 當前一季跟前季比（三個月加總）
    YOY 當月跟去年同月相比
    成長法:指標有兩個參數：(M=4 N=5)
    當我們要平滑月營收曲線時，取最近的 M 個值平均，產生新的曲線
    新的曲線連續 N 個月不斷變高
    均線法　成長法　need test
    """
    if date.day<10:
        date=date-relativedelta(months=1)
    df =income.get_by_stockid_date_months(r.stock_id,date,12) 
    if len(df.index)>=1:
        df['當月營收']=df['當月營收'].astype(float)
        df['去年當月營收']=df['去年當月營收'].astype(float)
        YOY=df.iloc[0]['去年同月增減(%)']
        if  len(df.index)>=12:  
            try:
                df['inavg3']=df['當月營收'].rolling(window=3,center=False,axis=0).mean()
                df['inavg12']=df['當月營收'].rolling(window=12,center=False,axis=0).mean()
                avg3=df.iloc[-1]['inavg3']
                avg12=df.iloc[-1]['inavg12']
                ratio1=avg3/avg12
            except:
                print(lno(),r.stock_id,df)
                pass
  
            try:
                df['inavg4']=df['當月營收'].rolling(window=4,center=False,axis=0).mean()
                length=len(df.index)
                result=1
                #print(lno(),df)
                for i in range(0,5):
                    #print(lno(),i,df.iloc[-1-i]['inavg4'],df.iloc[-2-i]['inavg4'])
                    if df.iloc[-1-i]['inavg4']<=df.iloc[-2-i]['inavg4']:
                        result=0
                        break
            except:        
                print(lno(),r.stock_id,df)
                pass
        #print(df['當月營收'].sum(),df['去年當月營收'].sum())
        #print(df)
        #print(lno(),ratio1,ratio2)
    #sys.exit()    
    return YOY,ratio1,result
        
def gen_analy_data(startdate,enddate,table_name,methods):
    stk,engine,con=init_sql()
    print(lno(),startdate,enddate,table_name,methods)
    cmd='SELECT * FROM "{}" WHERE date >= "{}" and date < "{}"'.format(table_name,startdate,enddate+relativedelta(days=1))
    df = pd.read_sql(cmd, con=con, parse_dates=['date']) 
    for func in methods:
        print(lno(),func.__name__)
        if func.__name__=='get_analy_1':
            df[['day5','day20','day60','市值(億)','MA5(角度)','MA21(角度)','MA89(角度)','ma1_ma5','ma1_ma21','ma5_ma21']]=df.apply(func,axis=1,result_type="expand")
        elif func.__name__=='get_big3_buy_vol':
            df[['外資','投信','自營商','三大法人']]=df.apply(func,axis=1,result_type="expand")
        elif func.__name__=='get_tdcc_dist_1000_400_buy_vol':
            df[['大戶買超','中戶買超']]=df.apply(func,axis=1,result_type="expand")  
        elif func.__name__=='get_income_ratio':
            df[['月營收YoY','月營收均線法','月營收成長法']]=df.apply(func,axis=1,result_type="expand")      
        else:    
            df[func.__name__]=df_apply_fun(df,func)    
    print(lno(),df)
    df.to_sql(name='{}_v1'.format(table_name), con=con, if_exists='replace', index=False,chunksize=10)
    raise
   
def longred_analy_mode(startdate,enddate,mode):
    
    global g_stk,g_engine
    if g_stk==None:
        g_stk=comm.stock_data()
    if g_engine==None:
        g_engine = create_engine('sqlite:///sql/buy_signal.db', echo=False)    
        
    con = g_engine.connect() 
    table_name='verylongred_v1'
    cmd='SELECT * FROM "{}" WHERE date >= "{}" and date < "{}"'.format(table_name,startdate,enddate+relativedelta(days=1))
    df = pd.read_sql(cmd, con=con, parse_dates=['date']) 

    print(lno(),len(df))
    #"""
    tStart = time.time()
    if mode=='upper_shadow':
        def upper_shadow(r):
            if platform.system().upper()=='LINUX':
                stk=comm.stock_data()
            else:
                stk=g_stk    
            df1=stk.get_df_by_enddate_num(r.stock_id,r.date,1)
            #print(lno(),r.date,df1)
            upper_shadow_val=df1.at[0,'high']-df1.at[0,'close']
            real_body=abs(df1.at[0,'close']-df1.at[0,'open'])
            if real_body==0:
                return np.nan     
            return upper_shadow_val/real_body
        df['上影線比例']=df_apply_fun(df,upper_shadow)    
        cond=[["市值(億)",100],['上影線比例',0.3]]
    elif mode=='over_prev_high':
        def over_prev_high(r):
            #print(lno(),r)
            if platform.system().upper()=='LINUX':
                stk=comm.stock_data()
            else:
                stk=g_stk    
            df1=stk.get_df_by_enddate_num(r.stock_id,r.date,20)
            #print(lno(),r.date,df1)
            try :
                over_phigh=df1.iloc[-1]['close']-df1.iloc[-2]['high']
            except:
                print(lno(),"some thing wrong",df1)
                #raise
                return np.nan     
            return over_phigh
        df['過昨日高']=df_apply_fun(df,over_prev_high)    
        cond=[["市值(億)",100],['過昨日高',0]]
    elif mode=='vol_ratio':
        def vol_ratio(r):
            #print(lno(),r)
            if platform.system().upper()=='LINUX':
                stk=comm.stock_data()
            else:
                stk=g_stk   
            df1=stk.get_df_by_enddate_num(r.stock_id,r.date,20)
            #print(lno(),r.date,df1)
            try :
                v_ratio=df1.iloc[-1]['vol']/df1.iloc[-2]['vol']
            except:
                print(lno(),"some thing wrong",df1)
                #raise
                return np.nan     
            return v_ratio
        df['量增比例']=df_apply_fun(df,vol_ratio)    
        cond=[["市值(億)",100],['量增比例',8,3,1]]    
    else:
        raise    
    tEnd = time.time()
    print ("It cost %.3f sec" % (tEnd - tStart))         
    
    #df.to_sql(name='verylongred_v1', con=con, if_exists='replace', index=False,chunksize=10)
    df_fin=pd.DataFrame()
    df_fin=gen_final_df(df_fin,'全部',df)
    df_fin=gen_cond_fin_df(df_fin,cond,df)   
    #cmp= df_fin.iloc[0]['5日勝率']
    #df_fin['5日勝率'] = df_fin['5日勝率'].apply(lambda x: f'<font color="red">{x}</font>'.format(x) if x>cmp else x) 
    #df_fin.style.applymap(show,subset=['5日勝率'])
    
    print(lno(),df_fin)    
"""
我永遠都在尋找四種股票
1.市值接近歷史低檔區的景氣循環股
  ex: 統一 冬買 夏賣
2.被大盤拖累PE回到歷史低點區的穩定獲利股
3.產品即將被大量採用的高成長股
  ex: 營收成長法 營收平均法 +市值<100億 + 莊家信號(投信買超 或 長紅K)
4.過去表現很好，進入轉型期股價下跌後即將回到上昇軌道的東山再起股

2.eps 歷史本益比低
3. 營收本月>20% 前月未增

1. 4 待考慮
參考 投信 基金績效 一年前10名 10大持股 新增 或是 持有 最近跌10% 投信開始買進
""" 

if __name__ == '__main__':
    print(mpl.get_configdir())
    sns.set()
    mpl.rcParams['font.family'] = 'sans-serif'
    mpl.rcParams[u'font.sans-serif'] = ['simhei']
    #mpl.rcParams['font.sans-serif'] = 'SimHei'
    
    if len(sys.argv)==1:
        startdate=stock_comm.get_date()
        down_fut_op_big3(startdate)
    elif sys.argv[1]=='-d' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==3 :
            # 從今日往前抓一個月
            enddate=datetime.strptime(sys.argv[2],'%Y%m%d')
            down_data(enddate)  
            #generate_final(enddate)  
        elif len(sys.argv)==4 :
            # 從今日往前抓一個月
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            down_gen_datas(startdate,enddate)      
        else :
              print (lno(),'func -p startdata enddate') 
    elif sys.argv[1]=='-g' :
        if len(sys.argv)==3 :
            #參數2:開始日期 
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            list =get_df_bydate(startdate)
            #df['外資buy']=df['外資buy'].astype('float64')            
            print(list)
        else :
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            df =get_dfs_bydate(startdate,enddate)
            print (lno(),df)  
    elif sys.argv[1]=='m1' :
        #投信 當日買超>200  市值<10億 or 位於歷史低本益比
        if len(sys.argv)==3 :
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            list =get_good_stock_m1(startdate)
            #df['外資buy']=df['外資buy'].astype('float64')            
            #print(lno(),list)
        else :
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            df =get_dfs_bydate(startdate,enddate)
            print (lno(),df)  
    elif sys.argv[1]=='f1' :
        if len(sys.argv)==4 :
            ##TODO: f1 find stock via find_point_K
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            #method='VeryLongRed_10day'
            func=find_point_K
            findstock_run_func(startdate,enddate,func)
    elif sys.argv[1]=='f2' :
        if len(sys.argv)==4 :
            ##TODO: f2 find stock 投信買超２００張
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            #method='VeryLongRed_10day'
            func=find_fund_buy
            findstock_run_func(startdate,enddate,func)
    elif sys.argv[1]=='f3' :  
        #TODO: f3 find 投信買超 v2      
        get_fund_buy_history()
    elif sys.argv[1]=='ff' :  
        #TODO: ff find  營收成長 營收平均    
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        gen_buy_list(startdate)
    elif sys.argv[1]=='ff1' :  
        #TODO: ff1 find  營收成長 營收平均    
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        gen_buy_list_step1(startdate)           
    elif sys.argv[1]=='longred' :
        if len(sys.argv)==4 :
            ## TODO find_stock long red generate
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            #method='LongRed_Breakthrough_10day'
            #method='LongRed_Breakthrough_20day'
            method='VeryLongRed_10day'
            find_stock=findstock()
            day=0
            nowdate=enddate
            while   nowdate>=startdate :
                nowdate = enddate - relativedelta(days=day)
                print(lno(),nowdate)
                find_stock.run(nowdate,method)
                day=day+1
    elif sys.argv[1]=='g1' :
        if len(sys.argv)==4 :
            ## TODO g1 gen_analy_data single
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            #method=[get_big3_buy_vol]
            #method=[get_tdcc_dist_1000_400_buy_vol] ## startdate 20150508
            method=[get_analy_1]
            #method=[mv5_vol_ratio,over_prev_high,upper_shadow,ma_tangled_day,get_income_ratio,get_big3_buy_vol]
            #table_name='find_point_K'
            table_name='find_fund_buy_v2'
            gen_analy_data(startdate,enddate,table_name,method)                         
    elif sys.argv[1]=='gg' :
        if len(sys.argv)==4 :
            ## TODO gg gen_analy_data
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            #method=[get_analy_1,mv5_vol_ratio,over_prev_high,upper_shadow,ma_tangled_day,get_income_ratio,get_big3_buy_vol]
            method=[get_analy_1]
            #method=[mv5_vol_ratio,over_prev_high,upper_shadow,ma_tangled_day,get_income_ratio,get_big3_buy_vol]
            #table_name='find_point_K'
            table_name='find_fund_buy_v2'
            gen_analy_data(startdate,enddate,table_name,method)                        
    elif sys.argv[1]=='r' :
        if len(sys.argv)==4 :
            ## TODO r report all 分析 大小市值 斜率
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            #method='find_point_K'
            method='find_fund_buy_v2'
            find_stock_analy(method,startdate,enddate)
    elif sys.argv[1]=='r1' :
        if len(sys.argv)==4 :
            ## TODO r1  verylongred_v1 report all 分析 大小市值 斜率
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            #find_stock=findstock()
            method='verylongred_v1'
            
            find_stock_analy(method,startdate,enddate)                        
    else: 
        ##TODO: need verify 
        ## 1股價站上月線2. KD低於20再上到50。3Macd翻正  
        ## 資金流 high -prev_low  and  prev_high -low  https://xstrader.net/%e7%a8%8b%e5%bc%8f%e4%ba%a4%e6%98%93%e5%9c%a8%e5%9f%ba%e9%87%91%e6%8a%95%e8%b3%87%e4%b8%8a%e7%9a%84%e6%87%89%e7%94%a8%e4%b9%8b%e4%ba%94-mfo%e8%b3%87%e9%87%91%e6%b5%81%e6%8c%87%e6%a8%99%e7%bf%bb/
        ##WVAD   https://xstrader.net/%e7%a8%8b%e5%bc%8f%e4%ba%a4%e6%98%93%e5%9c%a8%e5%9f%ba%e9%87%91%e6%8a%95%e8%b3%87%e4%b8%8a%e7%9a%84%e6%87%89%e7%94%a8%e4%b9%8b%e5%85%ad-k%e7%b7%9a%e7%a9%ba%e7%bf%bb%e5%a4%9a%ef%bc%88wvad%e6%8c%87/
        ##K值  https://xstrader.net/%e7%a8%8b%e5%bc%8f%e4%ba%a4%e6%98%93%e5%9c%a8%e5%9f%ba%e9%87%91%e6%8a%95%e8%b3%87%e4%b8%8a%e7%9a%84%e6%87%89%e7%94%a8%e4%b9%8b%e4%b9%9dk%e5%80%bc%e4%ba%a4%e6%98%93%e6%b3%95%e5%89%87/
        ##多頭 長下影線 收最高 需測是 https://xstrader.net/%e7%a8%8b%e5%bc%8f%e4%ba%a4%e6%98%93%e5%9c%a8%e5%9f%ba%e9%87%91%e6%8a%95%e8%b3%87%e4%b8%8a%e7%9a%84%e6%87%89%e7%94%a8%e4%b9%8b%e5%8d%81%e5%a4%9a%e9%a0%ad%e9%8e%9a%e5%ad%90/
        ## https://xstrader.net/%e7%a8%8b%e5%bc%8f%e4%ba%a4%e6%98%93%e5%9c%a8%e5%9f%ba%e9%87%91%e6%8a%95%e8%b3%87%e4%b8%8a%e7%9a%84%e6%87%89%e7%94%a8%e4%b9%8b14%e7%aa%81%e7%a0%b4%e4%b8%ad%e9%95%b7%e7%b7%9a%e7%9a%84%e7%b3%be/
        #葛拉罕提出了兩條挑股票的標準 1.總市值低於過去一年獲利的七倍 2.一個公司擁有的應該兩倍於它所欠下的。 https://xstrader.net/%e8%91%9b%e6%8b%89%e7%bd%95%e7%95%99%e4%b8%8b%e4%be%86%e7%9a%84%e6%8a%95%e8%b3%87%e6%99%ba%e6%85%a7/
        """
        單日交易次數 >平均交易(n day n=20))次數
        強弱指標 :商品漲跌幅－對應大盤漲跌幅。 
        
        根據韋爾達的定義，真實區間為下列三項中的最大值：(測量價格波動性的方法)
            當期最高價至最低價的幅度。
            當期最低價與前期收盤價的幅度。
            當期最高價至前期收盤價的幅度。
        v13=(close-open)/close*100;//漲跌幅
        v14=(close-low)/close*100;//
        v15=(high-close)/close*100;
        v16=(high-low)/close*100;
        v17=(high-open)/close*100;
        v18=(open-low)/close*100;  
        https://xstrader.net/%e8%a9%a6%e8%91%97%e7%94%a8%e7%a8%8b%e5%bc%8f%e4%be%86%e6%8f%8f%e8%bf%b0%e5%9e%8b%e6%85%8b%e4%b9%8b%e4%b8%80/  
        股價在近百日大跌五成但近十日主力卻逆向在收集籌碼的股票，把這個腳本去回測過去五年的所有股票，20天後出場，勝率超過六成，如果持有超過四十天，勝率更超過65%
        k線選股
        1.已過高點的優先
        2.在上昇趨勢中的優先
        3.比較輕盈的優先
        4.有量的優先
        5.打底很久剛冒出來的優先
        
        大跌過後
        //過去60個交易日投信曾五天買超過2000張
        //最近十天有六天以上，籌碼是收集的
        //大盤站上月均
        //最近三十天跌超過一成
        大跌後出現什麼K線型態可以進場？ https://xstrader.net/%e5%a4%a7%e8%b7%8c%e5%be%8c%e5%87%ba%e7%8f%be%e4%bb%80%e9%ba%bck%e7%b7%9a%e5%9e%8b%e6%85%8b%e5%8f%af%e4%bb%a5%e9%80%b2%e5%a0%b4%ef%bc%9f/
        參考 雞尾酒 https://xstrader.net/%e9%9b%9e%e5%b0%be%e9%85%92%e7%ad%96%e7%95%a5%e9%9b%b7%e9%81%94%e7%9a%84%e5%87%bd%e6%95%b8%e5%8c%96/        
        TODO: 大跌過後 抄底k線 大跌就是最好的利多
        https://xstrader.net/%e7%b6%9c%e5%90%88%e6%8a%84%e5%ba%95%e7%ad%96%e7%95%a5/
        大盤多空函數 : ex 外資10日 買6日以上 算多頭
        年底 投信做帳  https://xstrader.net/%e6%8a%95%e4%bf%a1%e5%b9%b4%e5%ba%95%e4%bd%9c%e5%b8%b3%e7%9a%84%e5%8f%af%e8%83%bd%e6%a8%99%e7%9a%84%e8%a6%81%e6%80%8e%e9%ba%bc%e6%8c%91/
        飆股條件 https://xstrader.net/2017%e9%a3%86%e8%82%a1%e7%9a%84%e9%95%b7%e7%9b%b8-%e4%bd%8e%e5%83%b9%ef%bc%8c%e4%b8%ad%e5%b0%8f%e5%9e%8b%ef%bc%8c%e8%bd%89%e6%a9%9f%ef%bc%8c%e5%88%a9%e5%9f%ba%e3%80%82/
        
        """
        pass
"""
https://xstrader.net/%e4%ba%a4%e6%98%93%e5%bf%83%e5%be%97%e7%ad%86%e8%a8%98/    
一，關於看盤
    看前30檔權值股的均線排列，如果大多數是多頭排列，就作多，如果大多數是空頭排列就作空
    觀察前幾檔股王股后們的表現，如果股王股后們都不行了，這個盤也就不行了。
    權值股的帶頭大哥們如果有漲停的，這盤繼續看多，如果有跌停的，後市堪慮。
    開盤很重要，看開盤再來決定今天站在買方還是賣方。
    期指常會做破底騙線.誘多誘空，不要太衝動，要看權值股是不是跟期指同樣破底
    利多不漲或利空不跌都是反轉的前兆
    盤中類股輪動的太厲害表示大家持股信心不強
    要看出量的股票是漲還跌，如果出量的股票一堆是下殺有量，今天就謝謝收看了
    拉上去回檔幅度小，撐在上面的時間長，再上的機率就大，拉上去後回檔幅度大，撐在上面的時間短，殺尾盤的機率就大
    開盤強的股票一路收上去，今天就有搞頭，開盤強的股票普遍壓回，要小心
["2330","2317","6505","2412","2882","1301","1303","1326","3008","2881","2454","2891","2002","1216","2311","2886","2912","2474","2382","2408","2892","5880","2357","2884","2207","4938","2880","2303","2105","2885"]
定存股:卜蜂，一零四，裕融，中保，中菲，和泰車及好樂迪。
//===========淨力指標==============
var:c4(0);
input:period2(10,"長期參數");
 
value12=summation(high-close,period2);//上檔賣壓
value13=summation(close-open,period2); //多空實績
value14=summation(close-low,period2);//下檔支撐
value15=summation(open-close[1],period2);//隔夜力道
if close<>0
then
value16=(value13+value14+value15-value12)/close*100;
0050 溢價指標 https://xstrader.net/0050%e6%ba%a2%e5%83%b9%e6%98%af%e5%ba%95%e9%83%a8%e6%8c%87%e6%a8%99%e5%97%8e/
大戶 散戶人數 決定空頭與否 https://xstrader.net/%e5%be%9e%e5%8d%83%e5%bc%b5%e5%a4%a7%e6%88%b6%e6%95%b8%e5%a2%9e%e6%b8%9b%e7%9c%8b%e5%a4%a7%e6%88%b6%e6%9c%89%e5%90%a6%e8%90%bd%e8%b7%91%ef%bc%81/

"""    
        