# -*- coding: utf-8 -*-
#from __future__ import unicode_literals
import io
import csv
import os
import time
import sys
#import urllib2
from datetime import datetime
from dateutil.relativedelta import relativedelta
#from grs import RealtimeWeight
import stock_comm
import stock_comm as comm 
import requests
import inspect
from inspect import currentframe, getframeinfo
import pandas as pd
from io import StringIO
from matplotlib import pyplot as plt
#import pyecharts
#from pyecharts import Kline
#from pyecharts import Candlestick
#import webbrowser
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)

def check_dst_folder(dstpath):
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath)     

                
    

def strip_dot(value):
    """
    转换字符串数字为int类型
     - 移除 ,
     - int
    """
    if type(value)==str:
        new_value = value.strip().replace(',', '')
        return int(new_value)
    else:
        return int(value)
def down_oneday(startdate):
    result=[]
    sr_list=[]
    dst_folder='csv/stock_netvalue'
    check_dst_folder(dst_folder)
    ti = startdate
    out_file='%s/%d%02d%02d.final'%(dst_folder,int(ti.year),int(ti.month),int(ti.day))
    #"""
    url='https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=csv&date=%d%02d%02d&selectType=ALL'%(int(ti.year),int(ti.month),int(ti.day))
    r = requests.get(url)
    if r.ok :
        r.encoding = 'big5hkscs'
        #print(lno(),r.text)
        #lines = r.text.replace('\r', '').split('\n').replace('"','')
        try :
            dfs = pd.read_csv(StringIO(r.text),skiprows=1)
        except:
            print(lno(),r.text)
            return
        dfs.columns = list(map(lambda l: l.replace(' ',''), dfs.columns))
        dfs.dropna(axis=1,how='all',inplace=True)
        dfs.dropna(thresh=2,inplace=True)
        #dfs.set_index('證券代號',inplace=True)
        print(lno(),len(dfs))
    #"""    
        url='https://www.tpex.org.tw/web/stock/aftertrading/peratio_analysis/pera_result.php?l=zh-tw&o=csv&d=%d/%02d/%02d&c=&s=0,asc'%(int(ti.year)-1911,int(ti.month),int(ti.day))
        r = requests.get(url)
        if r.ok :
            r.encoding = 'big5hkscs'
            #print(lno(),r.text)
            #lines = r.text.replace('\r', '').split('\n').replace('"','')
            dfs1 = pd.read_csv(StringIO(r.text),skiprows=3)
            dfs1.columns = [ '證券代號','證券名稱','本益比','每股股利','股利年度','殖利率(%)','股價淨值比']
            #dfs1.columns = list(map(lambda l: l.replace(' ',''), dfs1.columns))
            dfs1.dropna(axis=1,how='all',inplace=True)
            dfs1.dropna(thresh=2,inplace=True)
            #dfs1.set_index('證券代號',inplace=True)
            print(lno(),len(dfs1))
        
            df = pd.concat([dfs, dfs1], axis=0,join='inner',sort=False)    
            print(lno(),len(df))
            if len(df)!=0:
                df.to_csv(out_file,encoding='utf-8', index=False)
    #time.sleep(3)
        
        
def download(startdate,enddate):
    nowdatetime = enddate
    day=0
    while   nowdatetime>=startdate :
        down_oneday(nowdatetime)
        time.sleep(5)
        day=day+1
        nowdatetime = enddate - relativedelta(days=day)
        
def convert_K_stock(value):
    """
    轉換股數單位千股

    """
    return int(value/1000)
    
def get_stock_3big(stock_id,date,num,flag='tse'):
    dst_folder='csv/stock_big3'
    #print(lno(),date)
    if comm.stock_is_otc(stock_id,date)==1:
        flag='otc'
    nowdatetime = date
    day=0
    rec=0
    tmp_list=[]
    while   rec<num :
        nowdate = date - relativedelta(days=day)
        date_str='%d-%02d-%02d'%(int(nowdate.year), int(nowdate.month),int(nowdate.day))
        out_file='%s/%d%02d%02d.csv'%(dst_folder,int(nowdate.year), int(nowdate.month),int(nowdate.day))
        if flag=='otc':
            out_file='%s/%d%02d%02d_otc.csv'%(dst_folder,int(nowdate.year), int(nowdate.month),int(nowdate.day))
        day=day+1
        if os.path.exists(out_file): 
            
            df_s = pd.read_csv(out_file,encoding = 'utf-8')
            df_s.dropna(axis=1,how='all',inplace=True)
            df_s.dropna(inplace=True)
            #print(lno(),df_s[(df_s['date'] == date_str)].values.tolist())
            #print(lno(),df_s)
            df= df_s[(df_s['證券代號'] == stock_id)]

            df1=df[['外陸資買賣超股數(不含外資自營商)','投信買賣超股數','自營商買賣超股數','三大法人買賣超股數']]
            tt1=[]
            tt1.append(date_str)
            if len(df1)==1:
                tt1.extend(df1.iloc[0].values.tolist())
            else:
                tt1.extend([0,0,0,0])
            #df= df_s[(df_s['證券代號'] == '3481')]
            #print(lno(),tt1)
            tmp_list.append(tt1)
            rec=rec+1
        if day>num*2:
            break

    labels =['日期','外資','投信','自營商','三大法人買賣超']
    df_out = pd.DataFrame.from_records(tmp_list, columns=labels)
    
    #print(lno(),df_out)
    df_out['外資']=df_out['外資'].apply(convert_K_stock)
    df_out['投信']=df_out['投信'].apply(convert_K_stock)
    df_out['自營商']=df_out['自營商'].apply(convert_K_stock)
    #print(lno(),df_out)
    return df_out

def generate_stock_3big_pic(stock_no,enddate,outf):
    df=get_stock_3big(stock_no,enddate,5)
    df['date']=[comm.date_sub2time64(x) for x in df['日期'] ] 
    df=df.sort_values('date', ascending=True).reset_index(drop=True)    
    df_o=df[['日期','外資','投信','自營商']]
    index=df_o.index        
    bar_width = 0.3
    plt.figure(figsize=(10,8))
    A = plt.bar(index,df_o['外資'], bar_width, alpha=0.7,label="外資",color='red') 
    B = plt.bar(index+0.3,df_o['投信'],bar_width,alpha=0.7,label="投信",color='green') 
    C = plt.bar(index+0.6,df_o['自營商'],bar_width,alpha=0.7,label="自營商",color='blue') 
    def createLabels(data):
        for item in data:
            height = item.get_height()
            plt.text(
                item.get_x()+item.get_width()/2., 
                height*1.05, 
                '%.0f' % height,
                ha = "center",
                va = "bottom",
            )
    createLabels(A)
    createLabels(B)
    createLabels(C)
    plt.xticks(index+.3 / 2 ,df_o['日期'])
    plt.legend() 
    plt.grid(True)
    #plt.show()
    
    plt.savefig(outf)
    plt.clf()
    plt.close()  
 
if __name__ == '__main__':
    #print (lno(),sys.path[0])
    #get_cur_twii_list(datetime.today())
    if len(sys.argv)==1:
        startdate=stock_comm.get_date()
        enddate=stock_comm.get_date()
        down_stock_3big(startdate,enddate)
        down_stock_3big_otc(startdate,enddate)
        #generate_twse_3big(startdate,enddate)

    elif sys.argv[1]=='-d' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            download(startdate,enddate)
            #down_stock_3big_otc(startdate,enddate)
            #generate_twse_3big(startdate,enddate) 
        elif len(sys.argv)==3:
            print(lno())
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            down_oneday(startdate)
        
        else :
            print(lno(),'fun -d startdate enddate')
            

    elif sys.argv[1]=='-g' :
        print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            #參數2:開始日期 
            stock_id=sys.argv[2]
            startdate=datetime.strptime(sys.argv[3],'%Y%m%d')
            df =get_stock_3big(stock_id,startdate,5)
            
            #df['外資buy']=df['外資buy'].astype('float64')            
            
            print(lno(),df.loc[:,'外資'])
            print(lno(),df.loc[:,'外資'].values.tolist())
            
            

        else :
            print (lno(),'func -g date')         
    elif sys.argv[1]=='pic' :        
        stock_no=sys.argv[2]
        enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        generate_stock_3big_pic(stock_no,enddate,'tes1.png')
        #print(lno(),df)
    else:   
        objdatetime=datetime.strptime(sys.argv[1],'%Y%m%d')

        #print(lno(),df)
       
        