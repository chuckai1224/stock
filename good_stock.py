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
#from grs import RealtimeWeight

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
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from time import sleep
from selenium.webdriver.support.ui import Select
import shutil
import talib
from talib import abstract
import scipy.signal as signal 
from stocktool import comm as cm1 
import swifter
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
    fflag='tse'
    stock_3big_df=stock_big3.get_stock_3big_all(selday,'tse')
    #print( lno(),stock_3big_df)
    try:
        df= stock_3big_df[(stock_3big_df['投信買賣超股數'] >= 200000)][['證券代號','證券名稱','外陸資買賣超股數(不含外資自營商)','投信買賣超股數','自營商買賣超股數','三大法人買賣超股數']].reset_index(drop=True)
        #print( lno(),df.head())
        df_ex=comm.get_tse_exchange_data(selday)[[ 'stock_id','close']]
        #print( lno(),df_ex.head())
        df_ex.columns=['證券代號','close']
        df=pd.merge(df,df_ex)
        #print( lno(),df)
    except:
        print(lno(),selday,"無資料")
    otc_df=stock_big3.get_stock_3big_all(selday,'otc')
    #print(lno(),otc_df[['證券代號','證券名稱','外陸資買賣超股數(不含外資自營商)','投信買賣超股數','自營商買賣超股數','三大法人買賣超股數']])
    try:
        df1= otc_df[(otc_df['投信買賣超股數'] >= 200000)][['證券代號','證券名稱','外陸資買賣超股數(不含外資自營商)','投信買賣超股數','自營商買賣超股數','三大法人買賣超股數']]
        df_ex=comm.get_otc_exchange_data(selday)[[ 'stock_id','close']]
        df_ex.columns=['證券代號','close']
        df1=pd.merge(df1,df_ex)
    except:
        print(lno(),selday,"otc無資料")    
    #print(lno(),df.dtypes)
    #print(lno(),df1.dtypes)
    if len(df)==0 and len(df1)==0:
        return pd.DataFrame()
    elif len(df)==0 and len(df1)!=0:
        df2=df1
    elif len(df)!=0 and len(df1)==0:
        df2=df
    else:    
        df2=pd.concat([df,df1]).reset_index(drop=True)
    df2['close']=df2['close'].astype('float64')
    #print(lno(),df2)
    def get_total_stock(row):
        if len(row['證券代號'])!=4:
            return 0
        return tdcc_dist.get_total_stock(selday,row['證券代號'])
    df2['總股數']=df2.apply(get_total_stock, axis=1)
    
    df2['市值']=df2['總股數']*df2['close']
    df3=df2[(df2['市值'] <= 10000*1000000) & (df2['市值'] > 10000)]

    #print(lno(),df)
    #print(lno(),df1)
    if debug==1:
        print(lno(),selday,df3)
    return df3
    
    #d=comm.get_tse_exchange_data(selday)
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
                df.to_sql(name=self.strategy, con=con, if_exists='append', index=False,chunksize=10)    
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
    """
    shortaverage = average(close,s1);
    midaverage = average(close,s2);
    Longaverage = average(close,s3);
    value1= absvalue(shortaverage -midaverage);
    value2= absvalue(midaverage -Longaverage);
    value3= absvalue(Longaverage -shortaverage);
    value4= maxlist(value1,value2,value3);
    if value4*100 < Percent*Close
    and linearregangle(value4,5)<10
    then count=count+1;
    end;
    """ 
    ##TODO Q指標 https://xstrader.net/%E7%A8%8B%E5%BC%8F%E4%BA%A4%E6%98%93%E5%9C%A8%E5%9F%BA%E9%87%91%E6%8A%95%E8%B3%87%E4%B8%8A%E7%9A%84%E6%87%89%E7%94%A8%E4%B9%8B%E4%BA%8Cq%E6%8C%87%E6%A8%99/  
    def analy(self,method,startdate,enddate):
        ##TODO  len(df.index)==>len(df)
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

    def run_days(self):
        nowdate=self.rundate
        stk=comm.stock_data()

        day=0
        tStart = time.time()
        while day<10:
            nowdate=self.rundate -relativedelta(days=day)
            day=day+1
            d=comm.get_tse_exchange_data(nowdate)
           
            for j in range(0, len(d)):
                i=d.at[j,'stock_id']
                if comm.check_stock_id(i)==False:
                    continue
                ##平盤 收綠 不要
                try:
                    diff=float(d.at[j,'diff'])
                    if diff<=0:
                        continue    
                except:
                    print(lno(),d.iloc[j])
                    continue
                open=float(d.at[j,'open'])
                close=float(d.at[j,'close'])
                pre_close=close-diff
                if open >pre_close:
                    LongRedK_ratio= (close -pre_close)/pre_close
                else :
                    LongRedK_ratio= (close -open)/pre_close
                ##紅K幅度小於3% 不要    
                if LongRedK_ratio<=0.03:
                    continue
                high=float(d.at[j,'high'])
                upper_shadows_ratio=(high-close)/pre_close
                if (upper_shadows_ratio/LongRedK_ratio)>0.3:
                    continue
                flag='twse'    
                
                #print(lno(),i)
                """
                stk=comm.stock_data(i,engine)
                df=stk.get_df()
                """
                ##TODO test sql 
                #"""
                try:
                    df=pd.read_sql('select * from "{}"'.format(i),con=conn)
                    print(lno(),df.dtypes)
                    #print(lno(),df.head())
                    
                except:
                    print(lno(),i)
                    continue
                #"""    
                #df=comm.get_stock_df(i)
                #print(lno(),df.dtypes)
                #print(lno(),i,nowdate)
               
                df_now=comm.get_df_bydate_nums(df,240,self.rundate)
                period=20
                df_box=df_now.iloc[-period-1:-1]
                box_high=df_now.high.max()
                box_low=df_now.low.min()
                #print(lno(),close,box_high,box_low)
                #print(lno(),len(df_box))
                #print(lno(),df_now.tail(5))
                if close <=box_high:
                    continue
                #print(lno(),df_box)
                print(lno(),i,nowdate,box_high,box_low)
                #print(lno(),df_now.tail())
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
                 
                break
                #self.check_long_mode1(flag,i,self.rundate)
                
        tEnd = time.time()
        print ("It cost %.3f sec" % (tEnd - tStart))  
        pass
    def run_fun(self,rundate,fun,table_name):
        self.rundate=rundate
        markets=['tse','otc']
        tStart = time.time()
        out=[]
        for market in markets:
            if market=='tse':
                d=self.tse.get_df(self.rundate)
            else:
                d=self.otc.get_df(self.rundate)
                #print(lno(),out)
            def get_price_df(r):
                if comm.check_stock_id(r.stock_id)==False:
                    return
                print(lno(),r.stock_id,r.date)
                df=self.stk.get_df_by_enddate_num(r.stock_id,self.rundate,120)
                res=fun(df)
                if res>=1:
                    out.append([self.rundate,market,r.stock_id,'%s'%(table_name),res])  
            d.apply(get_price_df,axis=1)    
        print(lno(),out)    
        raise    
        #columns=['date','market','stock_id','買進信號','相似度']
        columns=self.columns
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
                df.to_sql(name=self.strategy, con=con, if_exists='append', index=False,chunksize=10)    
            #print(lno(),date_str)  
            #df1 = pd.read_sql('select * from "{}"'.format(date_str), con=con)  
            #print(lno(),df1)  
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
    _list=[name,d5_num,d5_ok/d5_num,d5_porfit/d5_num,d20_num,d20_ok/d20_num,d20_porfit/d20_num,d60_num,d60_ok/d60_num,d60_porfit/d60_num]
    print(lno(),d5_num,d20_num,d60_num)
    cols=['名稱','5日總數','5日勝率','5日獲利','20日總數','20日勝率','20日獲利','60日總數','60日勝率','60日獲利']
    dfo=pd.DataFrame([_list],columns=cols).round({'5日勝率': 2,'5日獲利': 5,'20日勝率': 2,'20日獲利': 5,'60日勝率': 2,'60日獲利': 5})
    df_fin=pd.concat([df_fin, dfo])
    return df_fin
    #return _list

def find_stock_analy(method,startdate,enddate):
    engine = create_engine('sqlite:///sql/buy_signal.db', echo=False)
    stk=comm.stock_data()
    con = engine.connect() 
    
    table_name='verylongred_v1'
    cmd='SELECT * FROM "{}" WHERE date >= "{}" and date <= "{}"'.format(table_name,startdate,enddate)
    df = pd.read_sql(cmd, con=con, parse_dates=['date']) 
    df_fin=pd.DataFrame()
    df_fin=gen_final_df(df_fin,'全部',df)
    df_fin=gen_final_df(df_fin,'大市值 MA89往上',df[(df.loc[:,"市值(億)"] >= 100) & (df.loc[:,"MA89(角度)"] >0 ) ] )
    df_fin=gen_final_df(df_fin,'大市值 MA89往下',df[(df.loc[:,"市值(億)"] >= 100) & (df.loc[:,"MA89(角度)"] <=0 ) ] )
    df_fin=gen_final_df(df_fin,'小市值 MA89往上',df[(df.loc[:,"市值(億)"] < 100) & (df.loc[:,"MA89(角度)"] >0 ) ] )
    df_fin=gen_final_df(df_fin,'小市值 MA89往下',df[(df.loc[:,"市值(億)"] < 100) & (df.loc[:,"MA89(角度)"] <=0 ) ] )
    df_fin=gen_final_df(df_fin,'小市值 MA89往上21往上',df[(df.loc[:,"市值(億)"] < 100) & (df.loc[:,"MA89(角度)"] >0 )& (df.loc[:,"MA21(角度)"] >0 ) ] )
    df_fin=gen_final_df(df_fin,'小市值 MA89往上21往下',df[(df.loc[:,"市值(億)"] < 100) & (df.loc[:,"MA89(角度)"] >0 )& (df.loc[:,"MA21(角度)"] <=0 ) ] )
    df_fin=gen_final_df(df_fin,'小市值 MA89往下21往上',df[(df.loc[:,"市值(億)"] < 100) & (df.loc[:,"MA89(角度)"] <=0 )& (df.loc[:,"MA21(角度)"] >0 ) ] )
    df_fin=gen_final_df(df_fin,'小市值 MA89往下21往下',df[(df.loc[:,"市值(億)"] < 100) & (df.loc[:,"MA89(角度)"] <=0 )& (df.loc[:,"MA21(角度)"] <=0 ) ] )
   
    print(lno(),df_fin)
    comm.to_html(df_fin,'out/buy_signal/{}_{}-{}fin.html'.format(table_name,startdate.strftime('%Y%m%d'),enddate.strftime('%Y%m%d') ))
def find_stock_ma_tangled(method,startdate,enddate):
    engine = create_engine('sqlite:///sql/buy_signal.db', echo=False)
    stk=comm.stock_data()
    con = engine.connect() 
    ##TODO get 盤整箱
    table_name='verylongred_v1'
    cmd='SELECT * FROM "{}" WHERE date >= "{}" and date <= "{}"'.format(table_name,startdate,enddate)
    df = pd.read_sql(cmd, con=con, parse_dates=['date']) 
    #print(lno(),df.duplicated())
    #print(lno(),df)
    #raise
    def get_correction_box(r):
        ##突破當日資料不要
        df1=stk.get_df_by_enddate_num(r.stock_id,r.date-relativedelta(days=1),120)
        
        ma_list = [5,10,20]
        tmp=[]
        for ma in ma_list:
            df1['MA_' + str(ma)] = talib.MA(df1.close,ma)
            #tmp.append(talib.LINEARREG_ANGLE(df1['MA_'+ str(ma)].values,ma)[-1])
        #print(lno(),df1.tail(5))
        def calc_sizeway(r):
            max_value=max(r.MA_5,r.MA_10,r.MA_20)
            min_value=min(r.MA_5,r.MA_10,r.MA_20)
            if min_value==0:
                return np.nan
            return (max_value-min_value)/min_value*100
        #df1['sizeway']=df1.apply(calc_sizeway,axis=1)
        sideway=df1.apply(calc_sizeway,axis=1)
        #tmp.append(talib.LINEARREG_ANGLE(df1['value4'].values,5)[-1])
        rev_sideway=sideway[::-1]
        #print(lno(),rev_sideway)
        cnt=0
        for i in rev_sideway:
            if i<=0.5:
                cnt=cnt+1
                continue
            break
        #sys.exit()
        if abs(cnt)>=5 :
            #print(lno(),r.stock_id,r.date,tmp)    
            #kline.show_stock_kline_pic(r.stock_id,r.date,120)
            pass
        return cnt
    df['ma_tangled_day']=df.swifter.apply(get_correction_box,axis=1)
    df.to_sql(name='verylongred_v2', con=self.con, if_exists='replace', index=False,chunksize=10)        
    df_fin=pd.DataFrame()
    df_fin=gen_final_df(df_fin,'全部',df)
    df_fin=gen_final_df(df_fin,'大市值 均線糾纏日期>=5',df[(df.loc[:,"市值(億)"] >= 100) & (df.loc[:,"ma_tangled_day"] >=5 ) ] )
    df_fin=gen_final_df(df_fin,'大市值 均線糾纏日期<5',df[(df.loc[:,"市值(億)"] >= 100) & (df.loc[:,"ma_tangled_day"] <5 ) ] )
    df_fin=gen_final_df(df_fin,'小市值 均線糾纏日期>=5',df[(df.loc[:,"市值(億)"] < 100) & (df.loc[:,"ma_tangled_day"] >=5 ) ] )
    df_fin=gen_final_df(df_fin,'小市值 均線糾纏日期<5',df[(df.loc[:,"市值(億)"] < 100) & (df.loc[:,"ma_tangled_day"] <5 ) ] )
    print(lno(),df_fin)
    comm.to_html(df_fin,'out/buy_signal/{}_{}-{}fin.html'.format("均線糾纏",startdate.strftime('%Y%m%d'),enddate.strftime('%Y%m%d') ))
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
def findstock_test(startdate,enddate):
    fs=findstock()
    nowdate=enddate
    day=0
    while   nowdate>=startdate :
        nowdate = enddate - relativedelta(days=day)
        print(lno(),nowdate)
        fs.run_fun(nowdate,find_ma1_cross_ma5,'test1')
        day=day+1 
    
"""
我永遠都在尋找四種股票
1.市值接近歷史低檔區的景氣循環股
2.被大盤拖累PE回到歷史低點區的穩定獲利股
3.產品即將被大量採用的高成長股
4.過去表現很好，進入轉型期股價下跌後即將回到上昇軌道的東山再起股

2.eps 歷史本益比低
3. 營收本月>20% 前月未增

1. 4 待考慮
參考 投信 基金績效 一年前10名 10大持股 新增 或是 持有 最近跌10% 投信開始買進
""" 
if __name__ == '__main__':
    ##TODO begin test
    
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
    elif sys.argv[1]=='t1' :
        if len(sys.argv)==4 :
            ## TODO 傳function 給run_fun 做股票篩選
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            #method='LongRed_Breakthrough_10day'
            #method='LongRed_Breakthrough_20day'
            #method='VeryLongRed_10day'
            method='test1'
            findstock_test(startdate,enddate)
    elif sys.argv[1]=='find' :
        if len(sys.argv)==4 :
            ## TODO find_stock test1 generate
            #method='test1'
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
    elif sys.argv[1]=='g2' :
        if len(sys.argv)==4 :
            ## TODO 長紅K 抓取斜率 市值
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            find_stock=findstock()
            method='VeryLongRed_10day'
            find_stock.analy(method,startdate,enddate) 
    elif sys.argv[1]=='g3' :
        if len(sys.argv)==4 :
            ## TODO 長紅K 抓取盤整區
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            find_stock=findstock()
            method='VeryLongRed_10day'
            find_stock_ma_tangled(method,startdate,enddate)         
    elif sys.argv[1]=='t2' :
        if len(sys.argv)==4 :
            ## TODO 分析 大小市值 斜率
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            find_stock=findstock()
            method='VeryLongRed_10day'
            find_stock_analy(method,startdate,enddate)            
    elif sys.argv[1]=='allsql' :  
        pass      
    else:   
        objdatetime=datetime.strptime(sys.argv[1],'%Y%m%d')
        #down_fut_op_big3(objdatetime)
        