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
from sqlalchemy import create_engine
import stock_comm as comm
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
def check(r):
    try:
        #print(lno(),r[0],type(r[0]))
        #print(lno(),r)
        if type(r['公司代號'])!=str:
            return 0
        if len(r['公司代號'])!=4:
            #print(lno(),r[0],type(r[0]))
            return 0
        #print(lno(),r['當月營收'],type(r['當月營收']))      
        if float(r['當月營收'])==0:  ##當月營收為0
            #print(lno(),r[0],type(r[0]))
            return 0
        #print(lno(),r[0],type(r[0]))          
        if float(r['前期比較增減(%)'])==0 or float(r['前期比較增減(%)'])==np.nan :  ##前期比較增減
            #print(lno(),r[0],type(r[0]))    
            return 0
        
        float(r['公司代號'])

        return 1
    except:
        #print(lno(),r[0],type(r[0]))
        return 0
      
        
class director:
    def __init__(self):
        self.engine = create_engine('sqlite:///sql/director.db', echo=False)
        self.con = self.engine.connect()
        self.data_folder='data/director' 
        check_dst_folder(self.data_folder)
        
    
    def get_by_stockid_date(self,stock_id,date):
        table_name=date.strftime('%Y%m')
        cmd='SELECT * FROM "{}" WHERE "公司代號" == "{}" '.format(table_name,stock_id)
        try:
            df = pd.read_sql(cmd, con=self.con) 
            print(lno(),df)
            return df
        except :
            print(lno(),stock_id,date,'get_by_stockid_date read sql fail')
            return pd.DataFrame()
    def get_by_stockid_date_months(self,stock_id,date,mons,debug=1):
        df_out= pd.DataFrame()
        for month in range (0,mons):
            nowdate=date-relativedelta(months=month)
            table_name=nowdate.strftime('%Y%m')
            #print(lno(),table_name)
            cmd='SELECT * FROM "{}" WHERE "公司代號" == "{}" '.format(table_name,stock_id)
            try:
                df = pd.read_sql(cmd, con=self.con)
                df['date']=nowdate
                if len(df_out.index)==0:
                    #print(lno(),df_out,df)
                    df_out=df
                else:
                    #print(lno(),df_out.head(1))
                    #print(lno(),df.head(1))
                    df_out=pd.concat([df_out,df])
                #print(lno(),df)
            except :
                if debug==1:
                    print(lno(),stock_id,table_name,'get_by_stockid_date read sql fail')
            
        return df_out.sort_values(by='date', ascending=True).reset_index(drop=True)
    def get_by_date(self,date):
        table_name=date.strftime('%Y%m')
        cmd='SELECT * FROM "{}" '.format(table_name)
        try :
            df = pd.read_sql(cmd, con=self.con) 
            #print(lno(),df)
            return df
        except :
            print(lno(),'get_by_date read sql fail')
            return pd.DataFrame()
    def df_to_sql(self,stock_id,df,date):
        enddate=date+relativedelta(days=1)
        table_name=stock_id
        if table_name in self.engine.table_names():
            cmd='SELECT * FROM "{}" WHERE date >= "{}" and date < "{}" '.format(table_name,date,enddate)
            df_query= pd.read_sql(cmd, con=self.con, parse_dates=['date'])
            if len(df_query):
                print(lno(),"repeat",date)
                return
            else:
                df.to_sql(name=table_name,  con=self.con, if_exists='append',  index= False,chunksize=10)
            
        else:    
            df.to_sql(name=table_name, con=self.con, if_exists='append',  index= False,chunksize=10)    

g_director=None    
def get_director():
    global g_director
    if g_director==None:
        print(lno())
        g_director=director()
    return g_director 
def down_stock_director(stock_id,market,date,download=1,debug=1):
    dst_folder='data/director/html/%s'%(stock_id)
    check_dst_folder(dst_folder)
    year=int(date.year)
    month=int(date.month)
    # 假如是西元，轉成民國
    if year>=1911:
        year=year-1911
    if debug==1:
        print(lno(),stock_id,year,month)
    str_ym='%d%02d'%(year,month)
    filename='%s/down_stock_director.%d%02d'%(dst_folder,year,month)
    # 偽瀏覽器
    
    headers = {'User-Agent: Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'}
    # 下載該年月的網站，並用pandas轉換成 dataframe
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    query_params = {
        'encodeURIComponent':'1',
        'step':'1',
        'firstin':'1',
        'off':'1',
        'keyword4':'',
        'code1':'',
        'TYPEK2':'',
        'checkbtn':'',
        'queryName':'co_id',
        'inpuType':'co_id',
        'TYPEK':'all',
        'isnew':'false',
        'co_id':stock_id,
        'year':year,
        'month':month,
    }
    url='https://mops.twse.com.tw/mops/web/ajax_stapap1'
    """
    url='https://mops.twse.com.tw/mops/web/ajax_stapap1?TYPEK=%s&firstin=true&year=%d&month=%02d&off=1&co_id=%s&step=0'%(market,year,month,stock_id)
    
    
    if os.path.exists(filename):
        return 
    if download==1 :
        print(lno(),url)
        try:
            #r = requests.post(url, data=query_params,timeout=(3.05, 27))
            r = requests.get(url, timeout=(3.05, 27))
        except:
            print(lno(),"ng")
            r.close() 
            return    
        if not r.ok:
            print(lno(),"Can not get data at {}".format(url))
            r.close() 
            return 
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in r:
                file.write(chunk)
        r.close()        
        time.sleep(5)        
                
    if not os.path.exists(filename): 
        return pd.DataFrame()
    try:    
        dfs = pd.read_html(filename,encoding = 'utf8')
    except:
        print(lno(),filename,"ng file")
        return pd.DataFrame()                
    if  len(dfs)>=5 :
        if str_ym in dfs[2].iloc[0][0]:
            print(lno(),str_ym,dfs[2].iloc[0][0])
            df=dfs[4]
        elif str_ym in dfs[3].iloc[0][0]:    
            df=dfs[5]
        else:
            print(lno(),"data gg")    
            return
        print(lno(),df.columns)
        columns=df[0].tolist()
        records=df[1].tolist()
        df1=pd.DataFrame([records],columns=columns)
        df1['date']=datetime(date.year,date.month,1)
        print(lno(),df1)
        _director=get_director()
        _director.df_to_sql(stock_id,df1,datetime(date.year,date.month,1))
        #print(lno(),dfs[4]['1'])
    else:    
        print(lno(),dfs)                
        print(lno(),len(dfs))
        print(lno(),str_ym,dfs[2].iloc[0][0])
        os.remove(filename)
        
 
            
    
def down_stock_director_goodinfo(stock_id,download=0,debug=1):
    dst_folder='data/director/goodinfo'
    check_dst_folder(dst_folder)
    if debug==1:
        print(lno(),stock_id)
    filename='%s/%s.html'%(dst_folder,stock_id)
    # 偽瀏覽器
  
    url='https://goodinfo.tw/StockInfo/StockDirectorSharehold.asp?STOCK_ID=%s'%(stock_id)
    header='user-agent: Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    cmd='curl -H "{}" {} -o {}'.format(header,url,filename)
    print(lno(),cmd)
    #if os.path.exists(filename):
    #    return 
    if download==1:
        os.system(cmd)
        print(lno(),"filesize:",os.path.getsize(filename))
        time.sleep(5)
        if os.path.getsize(filename)<2048:
            os.remove(filename)  
    ##only download
                       
    if not os.path.exists(filename): 
        return pd.DataFrame()
    try:    
        dfs = pd.read_html(filename,encoding = 'utf8')
    except:
        print(lno(),filename,"ng file")
        return pd.DataFrame()
    if '全體董監持股' in dfs[11].columns:
        df=dfs[11]
        df.columns=range(0,21)
        if debug==1:
            #print(lno(),dfs)  
            #print(lno(),len(dfs)  )
            #print(lno(),df) 
            #print(lno(),df[[0,15,16,17,20]])
            d=df[[0,15,16,17]].copy()
            d.columns=['date','全體董監持股張數','全體董監持股(%)','全體董監持股增減']
            def string_to_time(string):
                
                if '/' in string:
                    year, month = string.split('/')
                if '-' in string:
                    year, month = string.split('-')   
                if int(year)>=1911:
                    return datetime(int(year) , int(month), 1)
            d['date']=d['date'].apply(string_to_time)
            #d['date']= pd.to_datetime(d['date'], format='%Y/%m/%d')
            d=d.replace('-',np.NaN)
            d=d.dropna(thresh=2)
            print(lno(),d)
        comm.stock_df_to_sql(stock_id,'director',d)       

def get_stock_director_df_goodinfo(stock_id,download=1,debug=1):
    dst_folder='data/director/goodinfo'
    check_dst_folder(dst_folder)
    if debug==1:
        print(lno(),stock_id)
    filename='%s/%s.html'%(dst_folder,stock_id)
    # 偽瀏覽器
  
    url='https://goodinfo.tw/StockInfo/StockDirectorSharehold.asp?STOCK_ID=%s'%(stock_id)
    header='user-agent: Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    cmd='curl -H "{}" {} -o {}'.format(header,url,filename)
    #if os.path.exists(filename):
    #    return 
    if download==1:
        print(lno(),cmd)
        os.system(cmd)
        print(lno(),"filesize:",os.path.getsize(filename))
        time.sleep(5)
        if os.path.getsize(filename)<2048:
            os.remove(filename)  
    ##only download
                       
    if not os.path.exists(filename): 
        return pd.DataFrame()
    try:    
        dfs = pd.read_html(filename,encoding = 'utf8')
    except:
        print(lno(),filename,"ng file")
        return pd.DataFrame()
    if '全體董監持股' in dfs[11].columns:
        df=dfs[11]
        df.columns=range(0,21)
        d=df[[0,15,16,17]].copy()
        d.columns=['date','全體董監持股張數','全體董監持股(%)','全體董監持股增減']
        def string_to_time(string):
            if '/' in string:
                year, month = string.split('/')
            if '-' in string:
                year, month = string.split('-')   
            if int(year)>=1911:
                return datetime(int(year) , int(month), 1)
        d['date']=d['date'].apply(string_to_time)
        #d['date']= pd.to_datetime(d['date'], format='%Y/%m/%d')
        d=d.replace('-',np.NaN)
        d=d.dropna(thresh=2)
        return d.reset_index(drop=True)
    return pd.DataFrame()     
                        
def download_all_stock_director_goodinfo():
    nowdate=startdate
    date=datetime(2020,4,1)
    d1=comm.exchange_data('tse').get_df_date_parse(date)
    d1['market']='sii'
    d2=comm.exchange_data('otc').get_df_date_parse(date)
    d2['market']='otc'
    d=pd.concat([d1,d2])
    stock_list=d['stock_id'].tolist()
    for i in range(0,len(d)) :
        stock_id=d.iloc[i]['stock_id']
        if len(stock_id)!=4:
            continue
        if stock_id.startswith('00'):
            continue
        down_stock_director_goodinfo(stock_id)

def download_all_stock_director(startdate,enddate):
    nowdate=startdate
    date=datetime(2020,4,1)
    d1=comm.exchange_data('tse').get_df_date_parse(date)
    d1['market']='sii'
    d2=comm.exchange_data('otc').get_df_date_parse(date)
    d2['market']='otc'
    d=pd.concat([d1,d2])
    stock_list=d['stock_id'].tolist()
    while   nowdate<=enddate :
        
        for i in range(0,len(d)) :
            stock_id=d.iloc[i]['stock_id']
            market=d.iloc[i]['market']
            if len(stock_id)!=4:
                continue
            if stock_id.startswith('00'):
                continue
            down_stock_director(stock_id,market,nowdate)
        nowdate = nowdate + relativedelta(months=1) 
def parse_stock_director_xq(startdate,enddate):
    nowdate=startdate
    while   nowdate<=enddate :
        
        _csv='data/director/xq/director{}.csv'.format(nowdate.strftime('%Y%m'))
        if os.path.exists(_csv):
            dfs = pd.read_csv(_csv,encoding = 'big5hkscs',skiprows=5,header=None)
            try:
                #dfs.columns=['序號','stock_id','stock_name','成交','漲幅%','總量','董監持股佔股本比例']
                dfs.columns=['序號','stock_id','stock_name','成交','漲幅%','總量','d1','d2','董監持股','董監持股佔股本比例','符合條件數']
            except:
                print(lno(),dfs.iloc[0])
                dfs.columns=['序號','stock_id','stock_name','成交','漲幅%','總量','董監持股','董監持股佔股本比例','符合條件數']
                #raise
            #print(lno(),dfs.iloc[0])
            #raise    
            d=dfs[['stock_id','董監持股','董監持股佔股本比例']].copy()
            d['date']=datetime(nowdate.year,nowdate.month,15)
            for i in range(0,len(dfs)):
                print(lno(),d.iloc[i])
                #raise
                stock_id=d.iloc[i]['stock_id'].replace('.TW','')
                comm.stock_read_sql_add_df(stock_id,'director',d[i:i+1])
        else:
             print(lno(),_csv)   
        nowdate = nowdate + relativedelta(months=1)         
if __name__ == '__main__':

    sql_data=director()
    if len(sys.argv)==1:
        startdate=datetime.today().date()
        
        #nowdate=datetime(2020,2,1)
        #down_stock_director_goodinfo('6152')
        #download_all_stock_director_goodinfo()
        
        #down_stock_director('6152',nowdate)
        #"""
        startdate=datetime(2019,8,1)
        enddate=datetime(2020,3,1)
        parse_stock_director_xq(startdate,enddate)
        #"""
    elif sys.argv[1]=='xq' :
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        try:
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        except:
            enddate=startdate
        parse_stock_director_xq(startdate,enddate)
            
    elif sys.argv[1]=='-d' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            # 從今日往前抓一個月
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            now_date = startdate 
            while   now_date<=enddate :
                director.download(now_date) #new
                now_date = now_date + relativedelta(months=1)
        else :
              print (lno(),'func -d startdata enddate') 
         
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
            #get_revenue_by_stockid_bydate(stock_id,datatime)
            #get_revenue_by_stockid(stock_id,datatime)
            df=sql_data.get_by_date(datatime)
            print(lno(),df)
    elif sys.argv[1]=='sql' :
        
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        try:
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        except:
            enddate=startdate
        now_date = startdate 
        while   now_date<=enddate :
            #down_tse_monthly_report(int(now_date.year),int(now_date.month))
            #down_otc_monthly_report(int(now_date.year),int(now_date.month))
            sql_data.download(now_date)
            #gen_revenue_final_file(now_date)
            now_date = now_date + relativedelta(months=1)
   
        
    else:
        print (lno(),"unsport ")
        sys.exit()
    
    