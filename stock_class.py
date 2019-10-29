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
import stock_comm as cmm 
import requests
import inspect
from inspect import currentframe, getframeinfo
import pandas as pd
from bs4 import BeautifulSoup  
import re
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

 
                
def down_taiwan_dollar(startdate,enddate):

    dst_folder='csv/taiwan_dollar'
    filename='csv/taiwan_dollar/tmp.csv'
    out_file='csv/taiwan_dollar/taiwan_dollar_data.csv'
    check_dst_folder(dst_folder)
    startdate_str='%d/%02d/%02d'%(int(startdate.year), int(startdate.month),int(startdate.day))
    enddate_str='%d/%02d/%02d'%(int(enddate.year), int(enddate.month),int(enddate.day))
    url = 'http://www.taifex.com.tw/cht/3/dailyFXRateDown'
    query_params = {
        'queryStartDate': startdate_str,
        'queryEndDate': enddate_str
    }

    page = requests.post(url, data=query_params)

    if not page.ok:
        print(lno(),"Can not get data at {}".format(url))
        return 
    with open(filename, 'wb') as file:
        # A chunk of 128 bytes
        for chunk in page:
            file.write(chunk)
    df = pd.read_csv(filename,encoding = 'big5')
    df.dropna(axis=1,how='all',inplace=True)
    df.dropna(inplace=True)
    #print (lno(),df)
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
    
   
def get_taiwan_dollor(date):
    out_file='csv/taiwan_dollar/taiwan_dollar_data.csv'
    #print(lno(),date)
    if os.path.exists(out_file): 
        
        date_str='%d/%02d/%02d'%(int(date.year), int(date.month),int(date.day))
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print(lno(),df_s[(df_s['date'] == date_str)].values.tolist())
        df=df_s[(df_s['日期'] == date_str)]
        if len(df)==1:
            df.reset_index(inplace=True)
            #total=float(df.iat[0,'外資total'].strip().replace(',', ''))
            try:
                total=float(df.at[0,'美元／新台幣'])
            except:
                print (lno(),df.at[0,'美元／新台幣'])  
                total=0        
            
            #print(lno(),total_int)
            return total
        return 0
        
    else :
        return 0         
     
def down_stock_class1(startdate,enddate):

    dst_folder='csv/taiwan_dollar'
    filename='csv/taiwan_dollar/tmp.csv'
    out_file='csv/taiwan_dollar/taiwan_dollar_data.csv'
    check_dst_folder(dst_folder)
    startdate_str='%d/%02d/%02d'%(int(startdate.year), int(startdate.month),int(startdate.day))
    enddate_str='%d/%02d/%02d'%(int(enddate.year), int(enddate.month),int(enddate.day))
    url = 'https://www.moneydj.com/z/zg/zgd/zgd_E_E.djhtm'

def get_group(date):
    out_file='csv/group/group.csv'
    #print(lno(),date)
    if os.path.exists(out_file): 
        
        date_str='%d/%02d/%02d'%(int(date.year), int(date.month),int(date.day))
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print(lno(),df_s)
        for i in range(0,len(df_s)):
            #print(lno(),df_s.at[i,'groud_name'])
            stock_list=df_s.at[i,'stocks'].split(',')
            
            print(lno(),df_s.at[i,'groud_name'],len(stock_list),stock_list)
            for j in stock_list:
                print(lno(),j)
            break    
        return 0
        
    else :
        return 0     
        
if __name__ == '__main__':
    #print (lno(),sys.path[0])
    #get_cur_twii_list(datetime.today())
    if len(sys.argv)==1:
        #startdate=stock_comm.get_date()
        #down_taiwan_dollar(startdate,startdate)
        
        ##抓取集團名單
        ##再抓取集團股票名單,save group_stock.csv
        url = 'https://www.moneydj.com/z/zg/zgd/zgd_E_E.djhtm'
        r = requests.get(url, allow_redirects=True)
        if  r.ok:
            
            soup = BeautifulSoup(r.content.decode('cp950'), 'lxml')
            res=[]
            for option in soup.find_all('option'):
                tmp=[]
                if '集團' in option.text:
                    print ('value: {}, text: {}'.format(option['value'], option.text))
                    #https://www.moneydj.com/z/zg/zgd_EG00041_1.djhtm
                    url_stock='https://www.moneydj.com/z/zg/zgd_{}_1.djhtm'.format(option['value'])
                    r_stock = requests.get(url_stock, allow_redirects=True)
                    if not r_stock.ok:
                        print(lno(),"get fail")
                        continue
                    soup_stock = BeautifulSoup(r_stock.content.decode('cp950'), 'lxml')    
                    #script = soup_stock.find("script", text=pattern)
                    stocks=''
                    ids = soup_stock.find_all(id="oAddCheckbox")
                    print(lno(),len(ids))
                    for id in  ids:
                        #print(lno(),id)
                        stock=[]
                        #t=id.get_text().strip().replace('(',',').replace(')',',').split(',')
                        #print(lno(),id.get_text())
                        t=re.findall(r'[(](.*?)[)]', id.get_text())
                        if len(t)==0:
                            continue
                        print(lno(),len(t),t)
                        tt=t[0].split(',')
                        #print(lno(),len(tt),tt)
                        stock_id=tt[0].strip().replace('AS','').replace('\"','').replace('\'','')
                        stock_name=tt[1].strip().replace('\"','').replace('\'','')
                        stock.append(stock_id)
                        #stock.append(stock_name)
                        print(lno(),stock_id,stock_name)
                        if stocks=='':
                            stocks='{}'.format(stock_id)
                        else:
                            stocks='{},{}'.format(stocks,stock_id)
                    tmp.append(option.text)  #集團名稱
                    tmp.append(option['value'])  #網頁代號
                    tmp.append(stocks)
                    res.append(tmp)
                    
            print(lno(),res)
            labels = ['groud_name','group_id', 'stocks']
            df = pd.DataFrame.from_records(res, columns=labels)
            if not os.path.isdir('csv/group'):
                os.makedirs('csv/group')  
            out_file='csv/group/group.csv'
            df.to_csv(out_file,encoding='utf-8', index=False)
            
        
        #open('stock_class_v1.html', 'wb').write(r.content)
        #data=pd.read_html(url, flavor='bs4', header=0, encoding='UTF8')
        #data=pd.read_html(url)
        #print(lno(),r.content)
    elif sys.argv[1]=='-d' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            # 從今日往前抓一個月
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            down_taiwan_dollar(startdate,enddate)  
        else :
              print (lno(),'func -p startdata enddate') 
    elif sys.argv[1]=='-g' :
        if len(sys.argv)==3 :
            #參數2:開始日期 
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            dollar =get_taiwan_dollor(startdate)
            #df['外資buy']=df['外資buy'].astype('float64')            
            
            print(dollar)
            

        else :
            print (lno(),'func -g date')  
        
    
            
            
    else:   
        objdatetime=datetime.strptime(sys.argv[1],'%Y%m%d')
        get_group(objdatetime)
        
        
        