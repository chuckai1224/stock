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
def check_no_public(filename):
    with open(filename ,'r', encoding='utf-8') as f:
        if u'公開發行公司不繼續公開發行' in f.read():
            return True
    
    return False
        
    
def down_stock_director(stock_id,stock_name,market,date,download=1,debug=1):
    dst_folder='data/director/mops/%s'%(stock_id)
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
    header='user-agent: Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    url='https://mops.twse.com.tw/mops/web/ajax_stapap1?TYPEK=%s&firstin=true&year=%d&month=%02d&off=1&co_id=%s&step=0'%(market,year,month,stock_id)
    
    
    if not os.path.exists(filename):
        cmd='curl -H "{}" "{}" -o {} --speed-time 10 --speed-limit 1'.format(header,url,filename)
        print(lno(),cmd)
        if download==1:
            os.system(cmd)
            if not os.path.exists(filename):
                return pd.DataFrame()
             
            """
            try:
                if os.path.getsize(filename)<2048:
                    print(lno(),filename)
                    raise
                    os.remove(filename)  
                time.sleep(5)        
            except:
                print(lno(),stock_id,date,'not exist')
                return pd.DataFrame()    
            """    
    if not os.path.exists(filename): 
        return pd.DataFrame()
    if os.path.getsize(filename)<2048:
        if check_no_public(filename)==True:
            return pd.DataFrame()
        else:
            os.remove(filename) 
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
        df1['stock_id']=str(stock_id)
        df1['stock_name']=str(stock_name)
        
        df1['date']=datetime(date.year,date.month,1)
        print(lno(),df1)
        out_file='data/director/final/{}.csv'.format(stock_id)
        check_dst_folder('data/director/final')
        if os.path.exists(out_file): 
            print(lno(),out_file)
            df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype={'date': 'str','stock_id': 'str'})
            df_s.dropna(axis=1,how='all',inplace=True)
            df_s.dropna(inplace=True)
            df_s['date']=[comm.date_sub2time64(x) for x in df_s['date'] ]    
            df_s=df_s.append(df1,ignore_index=True)
            df_s.drop_duplicates(subset=['date'],keep='last',inplace=True)
            df_s=df_s.sort_values(by=['date'], ascending=False)
            df_s.to_csv(out_file,encoding='utf-8', index=False)
        else :
            df1.to_csv(out_file,encoding='utf-8', index=False)
        out_file='data/director/final/{}-{}.csv'.format(year,month)
        if os.path.exists(out_file): 
            print(lno(),out_file)
            df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype={'date': 'str','stock_id': 'str'})
            df_s.dropna(axis=1,how='all',inplace=True)
            df_s.dropna(inplace=True)
            df_s['date']=[comm.date_sub2time64(x) for x in df_s['date'] ] 
            df_s=df_s.append(df1,ignore_index=True)
            df_s.drop_duplicates(subset=['stock_id'],keep='last',inplace=True)
            df_s.to_csv(out_file,encoding='utf-8', index=False)
        else :
            df1.to_csv(out_file,encoding='utf-8', index=False)    
        #_director=get_director()
        #_director.df_to_sql(stock_id,df1,datetime(date.year,date.month,1))
        #print(lno(),dfs[4]['1'])
    else:    
        print(lno(),dfs)                
        print(lno(),len(dfs))
        print(lno(),str_ym,dfs[2].iloc[0][0])
        print(lno(),filename)
        raise
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
    while   nowdate<=enddate :
        date=datetime(nowdate.year,nowdate.month,1)+relativedelta(months=1,days=-1)
        d1=comm.exchange_data('tse').get_last_df_bydate(date)
        print(lno(),d1)
        d1['market']='sii'
        d2=comm.exchange_data('otc').get_last_df_bydate(date)
        print(lno(),d2)
        d2['market']='otc'
        d=pd.concat([d1,d2])
        stock_list=d['stock_id'].tolist()
        for i in range(0,len(d)) :
            
            stock_id=d.iloc[i]['stock_id']
            stock_name=d.iloc[i]['stock_name']
            market=d.iloc[i]['market']
            if len(stock_id)!=4:
                continue
            if stock_id.startswith('00'):
                continue
            down_stock_director(stock_id,stock_name,market,nowdate)
        nowdate = nowdate + relativedelta(months=1) 
        
def download_new_stock_director(startdate,enddate,dw_stock_id):
    date=datetime.today().date()
    d1=comm.exchange_data('tse').get_last_df_bydate(date)
    print(lno(),d1)
    d1['market']='sii'
    d2=comm.exchange_data('otc').get_last_df_bydate(date)
    print(lno(),d2)
    d2['market']='otc'
    d=pd.concat([d1,d2])
    stock_list=d['stock_id'].tolist()
    
    for i in range(0,len(d)) :
        stock_id=d.iloc[i]['stock_id']
        if dw_stock_id!=stock_id:
            continue
        stock_name=d.iloc[i]['stock_name']
        market=d.iloc[i]['market']
        if len(stock_id)!=4:
            continue
        if stock_id.startswith('00'):
            continue
        nowdate=startdate
        while   nowdate<=enddate :
            down_stock_director(stock_id,stock_name,market,nowdate)
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
def get_mops_month_df(date):
    _csv='data/director/final/{}-{}.csv'.format(date.year-1911,date.month)
    if os.path.exists(_csv):
        print(lno(),_csv)
        dfs = pd.read_csv(_csv,encoding = 'utf-8',dtype= {'stock_id':str})
        d=dfs[['stock_id','stock_name','全體董監持股合計']].copy()
        
        #print(lno(),d.iloc[0])
        return d
    return pd.DataFrame()
def get_mops_stock_director_df(r):
    _csv='data/director/final/{}.csv'.format(r['stock_id'])
    if os.path.exists(_csv):
        print(lno(),_csv)
        dfs = pd.read_csv(_csv,encoding = 'utf-8',dtype= {'stock_id':str})
        d=dfs[['date','stock_id','stock_name','全體董監持股合計']].copy()
        #print(lno(),d.iloc[0])
        return d
    return pd.DataFrame()
        
def get_xq_month_df(date):
    _csv='data/director/xq/director{}.csv'.format(date.strftime('%Y%m'))
    if os.path.exists(_csv):
        print(lno(),_csv)
        dfs = pd.read_csv(_csv,encoding = 'big5hkscs',skiprows=5,header=None)
        try:
            #dfs.columns=['序號','stock_id','stock_name','成交','漲幅%','總量','董監持股佔股本比例']
            dfs.columns=['序號','stock_id','stock_name','成交','漲幅%','總量','d1','d2','董監持股','董監持股佔股本比例','符合條件數']
        except:
            print(lno(),dfs.iloc[0])
            dfs.columns=['序號','stock_id','stock_name','成交','漲幅%','總量','董監持股','董監持股佔股本比例','符合條件數']
            #raise
        d=dfs[['stock_id','stock_name','董監持股','董監持股佔股本比例']].copy()
        
        #print(lno(),d.iloc[0])
        return d
    return pd.DataFrame()
        
        
def gen_director_good_list(date,debug=0,ver=1):
    nowdate=date
    cnt=0
    if ver==1:
        while cnt<=3:
            _csv='data/director/final/{}-{}.csv'.format(nowdate.year-1911,nowdate.month)
            if  os.path.exists(_csv):
                break
            nowdate=nowdate - relativedelta(months=1)
            cnt=cnt+1
        d=get_mops_month_df(nowdate)
        if len(d):
            prev_month = nowdate - relativedelta(months=1)
            d_prev=get_mops_month_df(prev_month)
            if len(d_prev):
                d_prev.columns=['stock_id','stock_name','前1月全體董監持股合計']
                df_out=pd.merge(d,d_prev)
                def calc_director_add(r):
                    try:
                        add= float(r['全體董監持股合計'])-float(r['前1月全體董監持股合計'])
                    except:
                        print(lno(),r) 
                        raise   
                    return add        
                df_out['董監持股增減']=df_out.apply(calc_director_add,axis=1)
                df_good=df_out[df_out['董監持股增減']>100*1000].copy().reset_index(drop=True)
                return df_good
        return pd.DataFrame() 
        raise      
    while cnt<=3:
        _csv='data/director/xq/director{}.csv'.format(nowdate.strftime('%Y%m'))
        if  os.path.exists(_csv):
            break
        nowdate=nowdate - relativedelta(months=1)
        cnt=cnt+1
    d=get_xq_month_df(nowdate)
    if len(d):
        prev_month = nowdate - relativedelta(months=1)
        d_prev=get_xq_month_df(prev_month)
        if len(d_prev):
            d_prev.columns=['stock_id','stock_name','前1月董監持股','前1月董監持股佔股本比例']
            df_out=pd.merge(d,d_prev)
            def calc_director_add(r):
                try:
                    add= float(r['董監持股'])-float(r['前1月董監持股'])
                except:
                    print(lno(),r) 
                    raise   
                return add        
            df_out['董監持股增減']=df_out.apply(calc_director_add,axis=1)
            df_good=df_out[df_out['董監持股增減']>100].copy().reset_index(drop=True)
            def removetw(r):
                return r['stock_id'].replace('.TW','')    
            df_good['stock_id']=df_good.apply(removetw,axis=1)
            
            return df_good
    else:
        nowdate=nowdate - relativedelta(months=1)
        cnt+=1
    return pd.DataFrame()        
           
    
        
        
                     
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
    elif sys.argv[1]=='mops' :
        ##公開資訊觀測站
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        try:
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        except:
            enddate=startdate
        download_all_stock_director(startdate,enddate)  
    elif sys.argv[1]=='stock' :
        ##公開資訊觀測站
        dw_stock=sys.argv[2]
        startdate=datetime.strptime(sys.argv[3],'%Y%m%d')
        try:
            enddate=datetime.strptime(sys.argv[4],'%Y%m%d')
        except:
            enddate=startdate
        download_new_stock_director(startdate,enddate,dw_stock) 
              
    elif sys.argv[1]=='mopsday' :
        prevmoth=datetime.today().date()-relativedelta(months=1)
        startdate=datetime(prevmoth.year,prevmoth.month,1)
        enddate=startdate
        download_all_stock_director(startdate,enddate)         
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
    
    