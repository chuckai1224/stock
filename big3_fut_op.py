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
import numpy as np
import op
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
def date_sub2time64(x):
    if '/' in x:
        fin=datetime.strptime(x,'%Y/%m/%d')
    else:    
        fin=datetime.strptime(x,'%Y-%m-%d')
    return np.datetime64(fin) 

def time64_Date(x):
    ts = pd.to_datetime(str(x)) 
    print(lno(),ts)
    d = ts.strftime('%Y%m%d')
    return d      
                
def down_fut_op_big3(enddate,download=1):

    dst_folder='csv/fut_op_big3'
    filename='csv/fut_op_big3/tmp.csv'
    
    check_dst_folder(dst_folder)
    enddate_str='%d/%02d/%02d'%(int(enddate.year), int(enddate.month),int(enddate.day))
    url = 'https://www.taifex.com.tw/cht/3/futAndOptDate'
    query_params = {
        'queryType': '1',
        'goDay': '',
        'dateaddcnt': '',
        'doQuery': '1',
        'queryDate': enddate_str
    }
    if download==1:
        page = requests.post(url, data=query_params)

        if not page.ok:
            print(lno(),"Can not get data at {}".format(url))
            return 
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in page:
                file.write(chunk)
    dfs = pd.read_html(filename,encoding = 'utf8')
    cnt=0
    for df in dfs :
        #print(lno(),df.shape)
        if df.shape[1]==13 :
            if '未平倉餘額' in  df.columns:
                print(lno(),df.iloc[0].values.tolist())
                print(lno(),df.iloc[1].values.tolist())
                print(lno(),df.iloc[2].values.tolist())
                print(lno(),df.iloc[3].values.tolist())

                out_file='csv/fut_op_big3/fut_op_big3_data_oi.csv'
                list=[]
                tmp=[]
                tmp.append(enddate_str)
                tmp.extend(df.iloc[0].values.tolist()[1:])
                tmp.extend(df.iloc[1].values.tolist()[1:])
                tmp.extend(df.iloc[2].values.tolist()[1:])
                list.append(tmp)
                print(lno(),len(list),list)
                labels=['日期']
                #"""
                labels+=['自oi多方期貨口數','自oi多方選擇權口數','自oi多方期貨契約金額','自oi多方選擇權契約金額']
                labels+=['自oi空方期貨口數','自oi空方選擇權口數','自oi空方期貨契約金額','自oi空方選擇權契約金額']
                labels+=['自oi多空淨額期貨口數','自oi多空淨額選擇權口數','自oi多空淨額期貨契約金額','自oi多空淨額選擇權契約金額']
                labels+=['投oi多方期貨口數','投oi多方選擇權口數','投oi多方期貨契約金額','投oi多方選擇權契約金額']
                labels+=['投oi空方期貨口數','投oi空方選擇權口數','投oi空方期貨契約金額','投oi空方選擇權契約金額']
                labels+=['投oi多空淨額期貨口數','投oi多空淨額選擇權口數','投oi多空淨額期貨契約金額','投oi多空淨額選擇權契約金額']
                #"""
                labels+=['外oi多方期貨口數','外oi多方選擇權口數','外oi多方期貨契約金額','外oi多方選擇權契約金額']
                labels+=['外oi空方期貨口數','外oi空方選擇權口數','外oi空方期貨契約金額','外oi空方選擇權契約金額']
                labels+=['外oi多空淨額期貨口數','外oi多空淨額選擇權口數','外oi多空淨額期貨契約金額','外oi多空淨額選擇權契約金額']
                print(lno(),len(labels))
                
                df1 = pd.DataFrame.from_records(list, columns=labels) 
                df1['日期']=[date_sub2time64(x) for x in df1['日期'] ]    
                print(lno(),df1)
                if os.path.exists(out_file): 
                    print(lno(),out_file)
                    df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype={'日期': 'str'})
                    df_s.dropna(axis=1,how='all',inplace=True)
                    df_s.dropna(inplace=True)
                    df_s['日期']=[date_sub2time64(x) for x in df_s['日期'] ]    
                    df_s=df_s.append(df1,ignore_index=True)
                    df_s.drop_duplicates(subset=['日期'],keep='last',inplace=True)
                    df_s=df_s.sort_values(by=['日期'], ascending=False)
                    df_s.to_csv(out_file,encoding='utf-8', index=False)
                else :
                    df1.to_csv(out_file,encoding='utf-8', index=False)

        if len(df)>4:
            if '未平倉餘額' in df.iloc[0].values.tolist() :
                #columns=df.iloc[3].values.tolist();
                #df.columns=columns
                #print (lno(),df.columns)
                df=df.drop([0,1,2,3])
                df=df.drop([0],axis=1).reset_index(drop=True)
                #print (lno(),df)
                if cnt==0:
                    #out_file='csv/fut_op_big3/fut_op_big3_data.csv'
                    #print(lno(),df)
                #elif cnt==1 :
                    #print(lno(),df)
                    out_file='csv/fut_op_big3/fut_op_big3_data_oi.csv'
                    list=[]
                    tmp=[]
                    tmp.append(enddate_str)
                    tmp.extend(df.iloc[0].values.tolist())
                    tmp.extend(df.iloc[1].values.tolist())
                    tmp.extend(df.iloc[2].values.tolist())
                    list.append(tmp)
                    print(lno(),len(list),list)
                    labels=['日期']
                    #"""
                    labels+=['自oi多方期貨口數','自oi多方選擇權口數','自oi多方期貨契約金額','自oi多方選擇權契約金額']
                    labels+=['自oi空方期貨口數','自oi空方選擇權口數','自oi空方期貨契約金額','自oi空方選擇權契約金額']
                    labels+=['自oi多空淨額期貨口數','自oi多空淨額選擇權口數','自oi多空淨額期貨契約金額','自oi多空淨額選擇權契約金額']
                    labels+=['投oi多方期貨口數','投oi多方選擇權口數','投oi多方期貨契約金額','投oi多方選擇權契約金額']
                    labels+=['投oi空方期貨口數','投oi空方選擇權口數','投oi空方期貨契約金額','投oi空方選擇權契約金額']
                    labels+=['投oi多空淨額期貨口數','投oi多空淨額選擇權口數','投oi多空淨額期貨契約金額','投oi多空淨額選擇權契約金額']
                    #"""
                    labels+=['外oi多方期貨口數','外oi多方選擇權口數','外oi多方期貨契約金額','外oi多方選擇權契約金額']
                    labels+=['外oi空方期貨口數','外oi空方選擇權口數','外oi空方期貨契約金額','外oi空方選擇權契約金額']
                    labels+=['外oi多空淨額期貨口數','外oi多空淨額選擇權口數','外oi多空淨額期貨契約金額','外oi多空淨額選擇權契約金額']
                    print(lno(),len(labels))
                    
                    df1 = pd.DataFrame.from_records(list, columns=labels) 
                    df1['日期']=[date_sub2time64(x) for x in df1['日期'] ]    
                    print(lno(),df1)
                    if os.path.exists(out_file): 
                        print(lno(),out_file)
                        df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype={'日期': 'str'})
                        df_s.dropna(axis=1,how='all',inplace=True)
                        df_s.dropna(inplace=True)
                        df_s['日期']=[date_sub2time64(x) for x in df_s['日期'] ]    
                        df_s=df_s.append(df1,ignore_index=True)
                        df_s.drop_duplicates(subset=['日期'],keep='last',inplace=True)
                        df_s=df_s.sort_values(by=['日期'], ascending=False)
                        df_s.to_csv(out_file,encoding='utf-8', index=False)
                    else :
                        df1.to_csv(out_file,encoding='utf-8', index=False)
                cnt=cnt+1
    #df.dropna(axis=1,how='all',inplace=True)
    #df.dropna(inplace=True)
    #print (lno(),df)
    """
    
    """
def down_fut_op_big3_bydate(startdate,enddate):
    #startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
    day=0
    nowdate=enddate
    while   nowdate>=startdate :
        nowdate = enddate - relativedelta(days=day)
        #print(lno(),nowdate)
        
        down_fut_op_big3(nowdate) 
        day=day+1   
def get_fix_delta(date,BoS,opprice,debug=0):
    list=op.get_Op_Data_df_list(date,0)
    

    if list!=[]:
        df1=list[1]
        if debug==3:
            print(lno(),df1[(df1['買賣權']==BoS)])
        df1=df1[['到期月份(週別)','買賣權','結算價','履約價']].copy()
        df1=df1.replace('-',np.NaN)
        df1.dropna(axis=1,how='all',inplace=True)
        #df1.dropna(inplace=True)
        df1['結算價']=df1['結算價'].astype(float)
        if debug==1:
            print(lno(),opprice)
        if BoS=='買權':
            df_lower =df1[(df1['買賣權']==BoS)&(df1['結算價']<=opprice)].reset_index(drop=True).head(1)
            df_upper =df1[(df1['買賣權']==BoS)&(df1['結算價']>=opprice)].reset_index(drop=True).tail(1)
            try:    
                lower_delta=op.get_delta_ratio(date,BoS,df_lower.iloc[0]['到期月份(週別)'],df_lower.iloc[0]['履約價'])
            except:
                print(lno(),opprice,BoS,df_lower,df1[(df1['買賣權']==BoS)].reset_index(drop=True))
                raise
            # 當沒有op價格大於opprice return lower_delta
            if len(df_upper)==0:
                return lower_delta
            try:
                upper_delta=op.get_delta_ratio(date,BoS,df_upper.iloc[0]['到期月份(週別)'],df_upper.iloc[0]['履約價'])
            except:
                print(lno(),"lower",df1[(df1['買賣權']==BoS)&(df1['結算價']<=opprice)].reset_index(drop=True).head(3))
                print(lno(),date,opprice,BoS,df_upper,df1[(df1['買賣權']==BoS)].reset_index(drop=True))
                raise    
        else:
            df_lower =df1[(df1['買賣權']==BoS)&(df1['結算價']<=opprice)].reset_index(drop=True).tail(1)
            df_upper =df1[(df1['買賣權']==BoS)&(df1['結算價']>=opprice)].reset_index(drop=True).head(1)
            ##op price no small price
            if len(df_lower)==0:
                return 0
            try:    
                lower_delta=op.get_delta_ratio(date,BoS,df_lower.iloc[0]['到期月份(週別)'],df_lower.iloc[0]['履約價'])
            except:
                print(lno(),"op price:",opprice,BoS,df_lower,df1[(df1['買賣權']==BoS)].reset_index(drop=True))
                raise
            # 當沒有op價格大於opprice return lower_delta
            if len(df_upper)==0:
                return lower_delta
            upper_delta=op.get_delta_ratio(date,BoS,df_upper.iloc[0]['到期月份(週別)'],df_upper.iloc[0]['履約價'])
        if debug==1:    
            if len(df_lower)==0:
                print(lno(),opprice,df1[(df1['買賣權']==BoS)&(df1['結算價']<=opprice)])
            if len(df_upper)==0:
                print(lno(),opprice,df1[(df1['買賣權']==BoS)&(df1['結算價']>=opprice)])     
        if debug==2:    
            print(lno(),df_lower)
            print(lno(),df_upper)
        
        
        if df_upper.iloc[0]['結算價']==df_lower.iloc[0]['結算價']:
            stepdiff=0
        else:
            stepdiff=(upper_delta-lower_delta)/(df_upper.iloc[0]['結算價']-df_lower.iloc[0]['結算價'])
        fix_delta=lower_delta+(opprice-df_lower.iloc[0]['結算價'])*stepdiff
        if debug==1:
            print(lno(),lower_delta,upper_delta,fix_delta)
        return fix_delta
    return 0    
def get_fut_op_big3_df_bydate(date,debug=0):
    out_file='csv/fut_op_big3/fut_op_big3_data_oi.csv'
    outcols=['日期','外期貨','外選擇權','自期貨','自選擇權','投期貨','投選擇權']
    #print(lno(),date)
    if os.path.exists(out_file): 
        date_str='%d-%02d-%02d'%(int(date.year), int(date.month),int(date.day))
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        #print(lno(),df_s[(df_s['date'] == date_str)].values.tolist())
        df=df_s[(df_s['日期'] == date_str)]
        if len(df)==1:
            df.reset_index(inplace=True)
            list=[]
            tmp=[]
            tmp.append(date_str)
            #total=float(df.iat[0,'外資total'].strip().replace(',', ''))
            opnums=df.iloc[0]['外oi多方選擇權口數']
            opcash=df.iloc[0]['外oi多方選擇權契約金額']
            opprice=opcash*1000/opnums/50
            fix_delta=get_fix_delta(date,'買權',opprice)
            fix_op2fut_b=fix_delta*opnums        
            if debug==1:
                print(lno(),"外資多",df.iloc[0]['外oi多方選擇權口數'],fix_delta,fix_op2fut_b)
            opnums=df.iloc[0]['外oi空方選擇權口數']
            opcash=df.iloc[0]['外oi空方選擇權契約金額']
            opprice=opcash*1000/opnums/50
            fix_delta=get_fix_delta(date,'賣權',opprice)
            fix_op2fut_s=fix_delta*opnums        
            if debug==1:
                print(lno(),"外資空",df.iloc[0]['外oi空方選擇權口數'],fix_delta,fix_op2fut_s)
            tmp.append(df.iloc[0]['外oi多方期貨口數']-df.iloc[0]['外oi空方期貨口數'])
            tmp.append(int((fix_op2fut_b-fix_op2fut_s)/4))
            
            opnums=df.iloc[0]['自oi多方選擇權口數']
            opcash=df.iloc[0]['自oi多方選擇權契約金額']
            opprice=opcash*1000/opnums/50
            fix_delta=get_fix_delta(date,'買權',opprice)
            fix_op2fut_b=fix_delta*opnums        
            if debug==1:
                print(lno(),fix_op2fut_b)
            
            opnums=df.iloc[0]['自oi空方選擇權口數']
            opcash=df.iloc[0]['自oi空方選擇權契約金額']
            opprice=opcash*1000/opnums/50
            fix_delta=get_fix_delta(date,'賣權',opprice)
            fix_op2fut_p=fix_delta*opnums        
            if debug==1:
                print(lno(),fix_op2fut_p)
            tmp.append(df.iloc[0]['自oi多方期貨口數']-df.iloc[0]['自oi空方期貨口數'])
            tmp.append(int((fix_op2fut_b-fix_op2fut_s)/4))
            
            opnums=df.iloc[0]['投oi多方選擇權口數']
            opcash=df.iloc[0]['投oi多方選擇權契約金額']
            if debug==1:
                print(lno(),df.iloc[0]['投oi多方選擇權口數'],df.iloc[0]['投oi多方選擇權契約金額'])
            if opnums!=0:
                opprice=opcash*1000/opnums/50
                fix_delta=get_fix_delta(date,'買權',opprice)
                fix_op2fut_b=fix_delta*opnums        
            else:
                fix_op2fut_b=0
            if debug==1:    
                print(lno(),fix_op2fut_b)
            
            opnums=df.iloc[0]['投oi空方選擇權口數']
            opcash=df.iloc[0]['投oi空方選擇權契約金額']
            if opnums!=0:
                opprice=opcash*1000/opnums/50
                fix_delta=get_fix_delta(date,'賣權',opprice)
                fix_op2fut_s=fix_delta*opnums        
            else:
                fix_op2fut_s=0
            if debug==1:    
                print(lno(),fix_op2fut_s)
            tmp.append(df.iloc[0]['投oi多方期貨口數']-df.iloc[0]['投oi空方期貨口數'])
            tmp.append(int((fix_op2fut_b-fix_op2fut_s)/4))
            #print(lno(),tmp)
            list.append(tmp)
            df1 = pd.DataFrame.from_records(list, columns=outcols) 
            df1['日期']=[date_sub2time64(x) for x in df1['日期'] ]    
            if debug==1:
                print(lno(),df1)
            return df1
        
        return pd.DataFrame(pd.np.empty(( 1, len(outcols))) * pd.np.nan, columns = outcols)
        
    else :
        return pd.DataFrame(pd.np.empty(( 1, len(outcols))) * pd.np.nan, columns = outcols)
     
def get_fut_op_big3_dfs_bydate(stardate,enddate):
    now_date = enddate
    day=0
    outcols=['日期','外期貨','外選擇權','自期貨','自選擇權','投期貨','投選擇權']
    df_s=pd.DataFrame(np.empty(( 1, len(outcols))) * np.nan, columns = outcols)
    while   now_date>=stardate :
        now_date = enddate - relativedelta(days=day)
        df=get_fut_op_big3_df_bydate(now_date) 
        df_s=df_s.append(df,ignore_index=True)
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        df_s.drop_duplicates(subset=['日期'],keep='last',inplace=True)
        df_s=df_s.sort_values(by=['日期'], ascending=False)
        day=day+1
    #print(lno(),comm.time64_DateTime(df_s.iloc[0]['日期']),type(comm.time64_DateTime(df_s.iloc[0]['日期'])))
    return df_s    
        
        
if __name__ == '__main__':
    #print (lno(),sys.path[0])
    #get_cur_twii_list(datetime.today())
    if len(sys.argv)==1:
        startdate=stock_comm.get_date()
        down_fut_op_big3(startdate)
    elif sys.argv[1]=='-d' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            # 從今日往前抓一個月
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            down_fut_op_big3_bydate(startdate,enddate)  
        else :
              print (lno(),'func -p startdata enddate') 
    elif sys.argv[1]=='-g' :
        if len(sys.argv)==3 :
            #參數2:開始日期 
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            list =get_fut_op_big3_df_bydate(startdate)
            #df['外資buy']=df['外資buy'].astype('float64')            
            
            print(list)
            

        else :
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            df =get_fut_op_big3_dfs_bydate(startdate,enddate)
            print (lno(),df)  
        
    
            
            
    else:   
        objdatetime=datetime.strptime(sys.argv[1],'%Y%m%d')
        down_fut_op_big3(objdatetime)
        