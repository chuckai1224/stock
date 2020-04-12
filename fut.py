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


save_path='data/fut/daydata'
headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language':'en-us,en;q=0.5',
    'Accept-Charset':'ISO-8859-1,utf-8;q=0.7,*;q=0.7'}

def down_data(enddate,download=1):

    dst_folder='data/fut'
    filename='data/fut/tmp.csv'
    out_file='data/fut/%d%02d%02d.csv'%(int(enddate.year), int(enddate.month),int(enddate.day))
    check_dst_folder(dst_folder)
    enddate_str='%d/%02d/%02d'%(int(enddate.year), int(enddate.month),int(enddate.day))
    url = 'https://www.taifex.com.tw/cht/3/futDailyMarketReport'


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

                  
##download 期貨收盤價
def down_fut_close(enddate,download=1,debug=0):
    filename='{}/down_fut_close.{}'.format(save_path,enddate.strftime('%Y%m%d'))
    check_dst_folder(save_path)
    datestr = enddate.strftime('%Y/%m/%d')    
    url = 'https://www.taifex.com.tw/cht/3/futDataDown '

    query_params = {
        'down_type': '1',
        'commodity_id':'TX',
        'commodity_id2':'',
        'queryStartDate': datestr,
        'queryEndDate': datestr
    }
    
    #print(lno(),download)
    if download==1:
        #print(lno(),url,query_params)
        try:
            page = requests.post(url, data=query_params)
        except:
            print(lno(),url)    
            raise
        if not page.ok:
            print(lno(),"Can not get data at {}".format(url))
            return 
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in page:
                file.write(chunk)
    if not os.path.exists(filename): 
        return pd.DataFrame()
    try:    
        df = pd.read_csv(filename,encoding = 'big5',skiprows=1,header=None)
    except:
        print(lno(),filename,"ng file")
        return pd.DataFrame()    
                
    #print(lno(),df.columns)
    df.columns=['交易日期', '契約', '到期月份(週別)', '開盤價', '最高價', '最低價', '收盤價', '漲跌價', '漲跌%',
       '成交量', '結算價', '未沖銷契約數', '最後最佳買價', '最後最佳賣價', '歷史最高價', '歷史最低價',
       '是否因訊息面暫停交易', '交易時段', '價差對單式委託成交量','dummy']
    df_f =df[(df['交易時段']=='一般')].reset_index(drop=True)
    if debug==1:
        print(lno(),len(df))
        print(lno(),df_f)
    return df_f
    
    return pd.DataFrame()

def down_fut_big3(enddate,download=1,debug=0):
    filename='{}/down_fut_big3.{}'.format(save_path,enddate.strftime('%Y%m%d'))
    check_dst_folder(save_path)
    datestr = enddate.strftime('%Y/%m/%d')    
    url = 'http://www.taifex.com.tw/cht/3/futContractsDate?queryType=1&goDay=&doQuery=1&dateaddcnt=&queryDate={}&commodityId='.format(datestr)
    query_params = {
        'queryType': '1',
        'goDay': '',
        'dateaddcnt':'',
        'queryDate': enddate.strftime("%Y/%m/%d"),
        'commodityId':''
    }
    """
    firstDate=enddate- relativedelta(years=3)
    lastDate=enddate
    url = 'http://www.taifex.com.tw/cht/3/futContractsDateDown?queryStartDate={}&queryEndDate={}&commodityId='.format(datestr,datestr)
    #http://www.taifex.com.tw/cht/3/futContractsDateDown？queryStartDate=2018%2F10%2F11&queryEndDate=2018%2F10%2F12&commodityId=TXF
    query_params = {
    'firstDate': '2017/03/19 00:00',
    'lastDate': '2019/03/19 00:00',
    'queryStartDate': datestr,
    'queryEndDate': datestr,
    'commodityId':''}
	"""
    if download==1:
        page = requests.post(url, data=query_params,headers = headers)
        if not page.ok:
            print(lno(),"Can not get data at {}".format(url))
            return 
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in page:
                file.write(chunk)
    if not os.path.exists(filename): 
        return
    print(lno(),"save file:",filename)
    #dfs = pd.read_csv(filename,encoding = 'utf8')
    #raise
    dfs = pd.read_html(filename,encoding = 'utf8')
    #print(lno(),len(dfs))
    if debug==2:
        for df in dfs :
            print(lno(),df)
      
    df=dfs[3]
    columns=['序號','商品名稱','身份別','多方口數','多方契約金額','空方口數','空方契約金額','多空淨額口數',
    '多空淨額契約金額','未平倉多方口數','未平倉多方契約金額','未平倉餘額空方口數','未平倉餘額空方契約金額',
    '未平倉餘額多空淨額口數','未平倉餘額多空淨額契約金額']
    df.columns=columns 
    df_f =df[(df['商品名稱']=='臺股期貨')|(df['商品名稱']=='小型臺指期貨')].reset_index(drop=True) 
    if debug==1:  
        print(lno(),df_f)  
    return df_f
def down_op_big3(enddate,download=1,debug=0):
    filename='{}/down_op_big3.{}'.format(save_path,enddate.strftime('%Y%m%d'))
    check_dst_folder(save_path)
    date_str=enddate.strftime("%Y/%m/%d")
    url = 'http://www.taifex.com.tw/cht/3/callsAndPutsDate?queryType=1&goDay=&doQuery=1&dateaddcnt=&queryDate={}&commodityId='.format(date_str)
    query_params = {
        'queryType': '1',
        'goDay': '',
        'doQuery':'1',
        'dateaddcnt':'',
        'queryDate': date_str,
        'commodityId':'TXO'
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
    if not os.path.exists(filename): 
        return
    print(lno(),"save file:",filename)
    dfs = pd.read_html(filename,encoding = 'utf8')
    #print(lno(),dfs)
    
    df=dfs[3]
    if len(df.columns)==16:
        df.columns=['序號','商品名稱','權別','身份別',
                    '買方口數','買方契約金額','賣方口數','賣方契約金額','買賣差額口數','買賣差額契約金額',
                    '未平倉買方口數','未平倉買方契約金額','未平倉賣方口數','未平倉賣方契約金額',
                    '未平倉買賣差額口數','未平倉買賣差額契約金額']
        if debug==1:
            print(lno(),df)
        df_f =df[(df['商品名稱']=='臺指選擇權')].reset_index(drop=True)
        if debug==1:
            print(lno(),df_f)
        return df_f
    return pd.Dataframe()    

def down_fut_top10(enddate,download=1,debug=0):
    filename='{}/down_fut_top10.{}'.format(save_path,enddate.strftime('%Y%m%d'))
    check_dst_folder(save_path)
    enddate_str=enddate.strftime("%Y/%m/%d")
    url = 'http://www.taifex.com.tw/cht/3/largeTraderFutQry?datecount=&contractId2=&queryDate={}&contractId=TX'.format(enddate_str)
    query_params = {
        'datecount': '',
        'contractId2': '',
        'queryDate': enddate_str,
        'contractId':'TX'
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
    if not os.path.exists(filename): 
        return
    
    dfs = pd.read_html(filename,encoding = 'utf8')
    if debug==1:
        cnt=0
        for i in dfs:
            print(lno(),cnt,i)
            cnt=cnt+1
       
    df=dfs[3]
    if debug==1:
        print(lno(),df)
        print(lno(),df.columns)
    
    columns=['契約名稱','到期月份(週別)',
            '買方前五大交易人合計(特定法人合計)部位數','買方前五大交易人合計(特定法人合計)百分比',
            '買方前十大交易人合計(特定法人合計)部位數','買方前十大交易人合計(特定法人合計)百分比',
            '賣方前五大交易人合計(特定法人合計)部位數','賣方前五大交易人合計(特定法人合計)百分比',
            '賣方前十大交易人合計(特定法人合計)部位數','賣方前十大交易人合計(特定法人合計)百分比',
            '全市場未沖銷部位數']
    df.columns=columns 
    df_f =df[(df['契約名稱']=='臺股期貨(TX+MTX/4)')].reset_index(drop=True) 
    if debug==1:  
        print(lno(),df_f)  
    return df_f
def down_op_top10(enddate,download=1,debug=0):
    filename='{}/down_op_top10.{}'.format(save_path,enddate.strftime('%Y%m%d'))
    check_dst_folder(save_path)
    enddate_str=enddate.strftime("%Y/%m/%d")
    
    url = 'http://www.taifex.com.tw/cht/3/largeTraderOptQry?datecount=&contractId2=&queryDate={}&contractId=TXO'.format(enddate_str)
    print(lno(),enddate_str)
    query_params = {
        'datecount': '',
        'contractId2': '',
        'queryDate': enddate_str,
        'contractId': 'TXO'
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
    if not os.path.exists(filename): 
        return
    
    dfs = pd.read_html(filename,encoding = 'utf8')
    df=dfs[3]
    if debug==2:
        print(lno(),df)
        #print(lno(),df.columns)
    #raise
    columns=['契約名稱','到期月份(週別)',
            '買方前五大交易人合計(特定法人合計)部位數',
            '買方前五大交易人合計(特定法人合計)百分比',
            '買方前十大交易人合計(特定法人合計)部位數',
            '買方前十大交易人合計(特定法人合計)百分比',
            '賣方前五大交易人合計(特定法人合計)部位數',
            '賣方前五大交易人合計(特定法人合計)百分比',
            '賣方前十大交易人合計(特定法人合計)部位數',
            '賣方前十大交易人合計(特定法人合計)百分比',
            '全市場未沖銷部位數']
    df.columns=columns 
    df_f =df[(df['契約名稱']=='臺指買權')|(df['契約名稱']=='臺指賣權')].reset_index(drop=True) 
    if debug==1:  
        print(lno(),df_f)  
    return df_f
"""
單位名稱            買進金額            賣出金額            買賣差額
0       自營商(自行買賣)     759,654,580   2,470,437,837  -1,710,783,257
1         自營商(避險)   7,048,660,595   7,478,180,543    -429,519,948
2              投信   2,412,612,070   1,074,835,090   1,337,776,980
3  外資及陸資(不含外資自營商)  50,198,590,409  52,511,853,689  -2,313,263,280
4           外資自營商         698,720         196,650         502,070
5              合計  60,419,517,654  63,535,307,159  -3,115,789,505
"""
def down_twse_big3(enddate,download=1,debug=1):
    
    check_dst_folder(save_path)
    enddate_str=enddate.strftime("%Y/%m/%d")
    filename='csv/big3/{}.csv'.format(enddate.strftime("%Y%m%d"))
    #if not os.path.exists(filename):
    url='http://www.twse.com.tw/fund/BFI82U?response=csv&dayDate=%d%02d%02d&type=day'%(int(enddate.year),int(enddate.month),int(enddate.day))
    print(lno(),enddate_str)
    if download==1:
        page = requests.get(url)
        if not page.ok:
            print(lno(),"Can not get data at {}".format(url))
            return 
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in page:
                file.write(chunk)
        time.sleep(3)        
    if not os.path.exists(filename): 
        return
    try:
        dfs = pd.read_csv(filename,encoding = 'big5',skiprows=1)
        dfs.dropna(axis=1,how='all',inplace=True)
        dfs.dropna(inplace=True)
        if debug==1:
            print(lno(),dfs)
    except:
        print(lno(),"df ng",filename)        
        raise    
    return dfs

def str2int(x):
    
    if type(x) != str:
        return x
    try:
        tmp=float(x.strip().replace(',',''))
    except:
        if '-' in x :
            return 0
        raise
    return tmp    

            
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
##v=2 算外資
def calc_fix_delta(enddate,BoS,price,df_top10,debug=0,v=1):
    ##抓取周月期別
    week=df_top10.iloc[0]['到期月份(週別)'].replace(' ','')
    month=df_top10.iloc[1]['到期月份(週別)'].replace(' ','')
    if debug ==1:
        print(lno(),price,week,month) 
    w_fix_delta=0
    m_fix_delta=0
    list=op.get_Op_Data_df_list(enddate,0)
    
    ## 計算月
    try:            
        df1=list[1][['到期月份(週別)','買賣權','結算價','履約價']].copy()
    except:
        print(lno(),enddate)
        raise    
    df1=df1.replace('-',np.NaN)
    df1.dropna(axis=1,how='all',inplace=True)
    df1['結算價']=df1['結算價'].astype(float)
    if BoS=='買權':
        df_lower =df1[(df1['買賣權']==BoS)&(df1['結算價']<=price)].reset_index(drop=True).head(1)
        df_upper =df1[(df1['買賣權']==BoS)&(df1['結算價']>=price)].reset_index(drop=True).tail(1)
    else:
        df_lower =df1[(df1['買賣權']==BoS)&(df1['結算價']<=price)].reset_index(drop=True).tail(1)
        df_upper =df1[(df1['買賣權']==BoS)&(df1['結算價']>=price)].reset_index(drop=True).head(1)
    try:    
        lower_delta=op.get_delta_ratio(enddate,BoS,df_lower.iloc[0]['到期月份(週別)'],df_lower.iloc[0]['履約價'])
    except:
        print(lno(),price,BoS,df_lower,df1[(df1['買賣權']==BoS)].reset_index(drop=True))
        raise
    try:
        upper_delta=op.get_delta_ratio(enddate,BoS,df_upper.iloc[0]['到期月份(週別)'],df_upper.iloc[0]['履約價'])
    except:
        print(lno(),enddate,price,BoS,df_upper,df1[(df1['買賣權']==BoS)].reset_index(drop=True))
        raise
    if df_upper.iloc[0]['結算價']==df_lower.iloc[0]['結算價']:
        stepdiff=0
    else:
        stepdiff=(upper_delta-lower_delta)/(df_upper.iloc[0]['結算價']-df_lower.iloc[0]['結算價'])
    m_fix_delta=lower_delta+(price-df_lower.iloc[0]['結算價'])*stepdiff
    if BoS=='買權':
        m_buy_call=str2int(df_top10.iloc[1]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
    else :
        m_buy_put=str2int(df_top10.iloc[4]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])   
    if debug==1:
        df_lower['delta']=lower_delta
        df_upper['delta']=upper_delta
        print(lno(),pd.concat([df_lower,df_upper]) )
        print(lno(),"op price=",price)
        print(lno(),"m_fix_delta",m_fix_delta) 
    if '-' not in week:
        ## 計算周
        ##  from price get op upper left
        df1=list[0][['到期月份(週別)','買賣權','結算價','履約價']].copy()
        df1=df1.replace('-',np.NaN)
        df1.dropna(axis=1,how='all',inplace=True)
        df1['結算價']=df1['結算價'].astype(float)

        if BoS=='買權':
            df_lower =df1[(df1['買賣權']==BoS)&(df1['結算價']<=price)].reset_index(drop=True).head(1)
            df_upper =df1[(df1['買賣權']==BoS)&(df1['結算價']>=price)].reset_index(drop=True).tail(1)
        else:
            df_lower =df1[(df1['買賣權']==BoS)&(df1['結算價']<=price)].reset_index(drop=True).tail(1)
            df_upper =df1[(df1['買賣權']==BoS)&(df1['結算價']>=price)].reset_index(drop=True).head(1)
        try:    
            lower_delta=op.get_delta_ratio(enddate,BoS,df_lower.iloc[0]['到期月份(週別)'],df_lower.iloc[0]['履約價'])
        except:
            print(lno(),price,BoS,df_lower,df1[(df1['買賣權']==BoS)].reset_index(drop=True))
            raise
        if len(df_upper)!=0:
            try:
                upper_delta=op.get_delta_ratio(enddate,BoS,df_upper.iloc[0]['到期月份(週別)'],df_upper.iloc[0]['履約價'])
            except:
                print(lno(),enddate,price,BoS,df_upper,df1[(df1['買賣權']==BoS)].reset_index(drop=True))
                raise
        else:
            df_upper= df_lower        
        if df_upper.iloc[0]['結算價']==df_lower.iloc[0]['結算價']:
            stepdiff=0
        else:
            stepdiff=(upper_delta-lower_delta)/(df_upper.iloc[0]['結算價']-df_lower.iloc[0]['結算價'])
        w_fix_delta=lower_delta+(price-df_lower.iloc[0]['結算價'])*stepdiff
        
        if BoS=='買權':
            w_buy_call=str2int(df_top10.iloc[0]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
            final_delta=(w_buy_call*w_fix_delta+m_buy_call*m_fix_delta)/(w_buy_call+m_buy_call)
            if debug==1:
                print(lno(),'w top10 buy call:',w_buy_call,w_buy_call*100/(w_buy_call+m_buy_call))
                print(lno(),'m top10 buy call:',m_buy_call,m_buy_call*100/(w_buy_call+m_buy_call))
                
        else :
            w_buy_put=str2int(df_top10.iloc[3]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])    
            final_delta=(w_buy_put*w_fix_delta+m_buy_put*m_fix_delta)/(w_buy_put+m_buy_put)
            if debug==1:
                print(lno(),'w top10 buy put:',w_buy_put,w_buy_put*100/(w_buy_put+m_buy_put))
                print(lno(),'m top10 buy put:',m_buy_put,m_buy_put*100/(w_buy_put+m_buy_put))
        if debug==1:
            df_lower['delta']=lower_delta
            df_upper['delta']=upper_delta
            print(lno(),pd.concat([df_lower,df_upper]) )
            print(lno(),"op price=",price)
            print(lno(),"w_fix_delta",w_fix_delta) 
            print(lno(),"final_delta",final_delta)      
    else:
        final_delta=m_fix_delta 
        if debug==1:
            print(lno(),"final_delta",final_delta)    
    if v==2:
        ##外資 need w m fix delta
        return w_fix_delta,m_fix_delta,final_delta
    else:                 
        return final_delta

#計算 莊家 壓力 支撐
def calc_pressure_support(enddate,BoS,price,df_top10,debug=0):
    week=df_top10.iloc[0]['到期月份(週別)'].replace(' ','')
    month=df_top10.iloc[1]['到期月份(週別)'].replace(' ','')
    if debug ==1:
        print(lno(),price,week,month) 
    list=op.get_Op_Data_df_list(enddate,0)
    ## 月 op data            
    df1=list[1][['到期月份(週別)','買賣權','結算價','履約價']].copy()
    df1=df1.replace('-',np.NaN)
    df1.dropna(axis=1,how='all',inplace=True)
    df1['結算價']=df1['結算價'].astype(float)
    df1['履約價']=df1['履約價'].astype(float)
    if BoS=='買權':
        df_lower =df1[(df1['買賣權']==BoS)&(df1['結算價']<=price)].reset_index(drop=True).head(1)
        df_upper =df1[(df1['買賣權']==BoS)&(df1['結算價']>=price)].reset_index(drop=True).tail(1)
    else:
        df_lower =df1[(df1['買賣權']==BoS)&(df1['結算價']<=price)].reset_index(drop=True).tail(1)
        df_upper =df1[(df1['買賣權']==BoS)&(df1['結算價']>=price)].reset_index(drop=True).head(1)
    if len(df_upper)==0:
        stepdiff=0
    elif df_upper.iloc[0]['結算價']==df_lower.iloc[0]['結算價']:
        stepdiff=0
    else:
        stepdiff=(df_upper.iloc[0]['履約價']- df_lower.iloc[0]['履約價'])/(df_upper.iloc[0]['結算價']- df_lower.iloc[0]['結算價'])
    m_pressure_support=(price- df_lower.iloc[0]['結算價'])*stepdiff+df_lower.iloc[0]['履約價']
    
    if BoS=='買權':
        m_sell_call=str2int(df_top10.iloc[1]['賣方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
    else :
        m_sell_put=str2int(df_top10.iloc[4]['賣方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])   
    if debug!=0:
        print(lno(),pd.concat([df_lower,df_upper]) )
        print(lno(),"op price=",price)
        print(lno(),"月支撐壓力",m_pressure_support) 
    if '-' not in week:
        ##  from price get op upper left
        df1=list[0][['到期月份(週別)','買賣權','結算價','履約價']].copy()
        df1=df1.replace('-',np.NaN)
        df1.dropna(axis=1,how='all',inplace=True)
        df1['結算價']=df1['結算價'].astype(float)
        df1['履約價']=df1['履約價'].astype(float)
        if BoS=='買權':
            df_lower =df1[(df1['買賣權']==BoS)&(df1['結算價']<=price)].reset_index(drop=True).head(1)
            df_upper =df1[(df1['買賣權']==BoS)&(df1['結算價']>=price)].reset_index(drop=True).tail(1)
        else:
            df_lower =df1[(df1['買賣權']==BoS)&(df1['結算價']<=price)].reset_index(drop=True).tail(1)
            df_upper =df1[(df1['買賣權']==BoS)&(df1['結算價']>=price)].reset_index(drop=True).head(1)
        
        if len(df_upper)==0:
            stepdiff=0
        elif df_upper.iloc[0]['結算價']==df_lower.iloc[0]['結算價']:
            stepdiff=0
        else:
            stepdiff=(df_upper.iloc[0]['履約價']- df_lower.iloc[0]['履約價'])/(df_upper.iloc[0]['結算價']- df_lower.iloc[0]['結算價'])
        w_pressure_support=(price- df_lower.iloc[0]['結算價'])*stepdiff+df_lower.iloc[0]['履約價']
        
        if BoS=='買權':
            w_sell_call=str2int(df_top10.iloc[0]['賣方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
            final=(w_sell_call*w_pressure_support+m_sell_call*m_pressure_support)/(w_sell_call+m_sell_call)
        else :
            w_sell_put=str2int(df_top10.iloc[3]['賣方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])    
            final=(w_sell_put*w_pressure_support+m_sell_put*m_pressure_support)/(w_sell_put+m_sell_put)
        if debug==1:
            print(lno(),pd.concat([df_lower,df_upper]) )
            print(lno(),"op price=",price)
            print(lno(),"周支撐壓力",w_pressure_support) 
            #print(lno(),"final",final)      
    else:
        final=m_pressure_support 
        w_pressure_support=np.nan
        if debug==1:
            print(lno(),"final",final)    
    ## 應該return 周 月 壓力(支撐)             
    return w_pressure_support,m_pressure_support


def down_fut_op_big3_top10_data_bydate(enddate,download=1,debug=0xff):
    outcols=['日期','期貨收盤價',#0
            '外資大小台','外資 op約當大台','外資 月支撐','外資 月壓力','外資 周支撐','外資 周壓力',#1#1
            '外資 buy call num','外資 buy call cash','外資 buy put num','外資 buy put cash',#5
            '外資 sell call num','外資 sell call cash','外資 sell put num','外資 sell put cash',#9
            '自營商大小台','自營商 op約當大台','自營商 月支撐','自營商 月壓力','自營商 周支撐','自營商 周壓力',#10
            '自營商 buy call num','自營商 buy call cash','自營商 buy put num','自營商 buy put cash',#14
            '自營商 sell call num','自營商 sell call cash','自營商 sell put num','自營商 sell put cash',#18
            '投信大小台','投信 op約當大台','投信 月支撐','投信 月壓力','投信 周支撐','投信 周壓力',#19
            '投信 buy call num','投信 buy call cash','投信 buy put num','投信 buy put cash',#23
            '投信 sell call num','投信 sell call cash','投信 sell put num','投信 sell put cash',#27
            '十大 月淨多單','十大 所有淨多單',#29
            '十大 周buy call 口數','十大 周buy put 口數',#31
            '十大 月buy call 口數','十大 月buy put 口數',#33
            '十大 周sell call 口數','十大 周sell put 口數',#35
            '十大 月sell call 口數','十大 月sell put 口數',#37
            '十大 所有buy call 口數','十大 所有buy put 口數',#39
            '十大 所有sell call 口數','十大 所有sell put 口數',#41
            '自營商(自行買賣)買賣差額',
            '自營商(避險)買賣差額',
            '投信買賣差額',
            '外資及陸資買賣差額',
            '外資自營商買賣差額',
            'w call delta','w put delta',
            'm call delta','m put delta',
            '十大 周op約當大台','十大 月op約當大台'
            ]
    f_big3_cols=[]        
    s_big3_cols=[]
    t_big3_cols=[]
    top10_cols=[]
    for i in  outcols:
        if '外資' in i:
            f_big3_cols.append(i)
        elif '自營商' in i:
            s_big3_cols.append(i)
        elif '投信' in i:
            t_big3_cols.append(i)                 
        elif '十大' in i:
            top10_cols.append(i)

    df_o = pd.DataFrame(np.empty(( 1, len(outcols))) * np.nan, columns = outcols)   
    #raise
    dst_folder='final'
    out_file='final/fut_day_report_fin.csv'
    check_dst_folder(dst_folder)
    dw=1
    df_fut_close=down_fut_close(enddate,download=dw,debug=1)
    if len(df_fut_close)!=0:
        if df_fut_close.iloc[0]['交易日期']!=enddate.strftime('%Y/%m/%d'):
            print(lno(),enddate,"no data")
            return
        df_twse_big3=down_twse_big3(enddate,download=dw,debug=1)
        df_fut_big3=down_fut_big3(enddate,download=dw,debug=1)
        df_op_big3=down_op_big3(enddate,download=dw,debug=1)
        df_fut_top10=down_fut_top10(enddate,download=dw,debug=1)
        df_op_top10=down_op_top10(enddate,download=dw,debug=1)
        
        op.down_optData(enddate,enddate)
        if not os.path.exists('csv/op/op_delta_{}.csv'.format(enddate.strftime('%Y%m%d'))): 
            op.down_opDelta(enddate)
        print(lno(),df_fut_close.iloc[0]['收盤價'])
        
        df_o.iloc[0]['期貨收盤價']=df_fut_close.iloc[0]['收盤價']
        ##現貨 三大法人
        df=df_twse_big3
        if debug ==1:
            print(lno(),df)
        df_o.iloc[0]['自營商(自行買賣)買賣差額']=str2int(df[df['單位名稱'] == '自營商(自行買賣)'].iloc[0]['買賣差額'])
        df_o.iloc[0]['自營商(避險)買賣差額']=str2int(df[df['單位名稱'] == '自營商(避險)'].iloc[0]['買賣差額'])
        df_o.iloc[0]['投信買賣差額']=str2int(df[df['單位名稱'] == '投信'].iloc[0]['買賣差額'])
        #if enddate > datetime(2020, 2, 21):
        df_o.iloc[0]['外資及陸資買賣差額']=str2int(df[df['單位名稱'] == '外資及陸資(不含外資自營商)'].iloc[0]['買賣差額'])
        df_o.iloc[0]['外資自營商買賣差額']=str2int(df[df['單位名稱'] == '外資自營商'].iloc[0]['買賣差額'])
        if debug ==1:
            print(lno(),df_o.iloc[0]['自營商(自行買賣)買賣差額'])
            print(lno(),df_o.iloc[0]['自營商(避險)買賣差額'])
            print(lno(),df_o.iloc[0]['投信買賣差額'])
            print(lno(),df_o.iloc[0]['外資及陸資買賣差額'])
            print(lno(),df_o.iloc[0]['外資自營商買賣差額'])
        
        #10大 期貨
        if len(df_fut_top10)==3:
            # 十大月買方 - 十大月買方
            df_o.iloc[0]['十大 月淨多單']=str2int(df_fut_top10.iloc[1]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])-str2int(df_fut_top10.iloc[1]['賣方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
            # 十大所有買方 - 十大所有買方
            df_o.iloc[0]['十大 所有淨多單']=str2int(df_fut_top10.iloc[2]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])-str2int(df_fut_top10.iloc[2]['賣方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
        #10大 op    
        if len(df_op_top10)==6:
            """
            '契約名稱', '到期月份(週別)', 
            '買方前五大交易人合計(特定法人合計)部位數', '買方前五大交易人合計(特定法人合計)百分比',
            '買方前十大交易人合計(特定法人合計)部位數', '買方前十大交易人合計(特定法人合計)百分比',
            '賣方前五大交易人合計(特定法人合計)部位數', '賣方前五大交易人合計(特定法人合計)百分比',
            '賣方前十大交易人合計(特定法人合計)部位數', '賣方前十大交易人合計(特定法人合計)百分比', 
            '全市場未沖銷部位數'
            """
            filter=['契約名稱', '到期月份(週別)','買方前十大交易人合計(特定法人合計)部位數','賣方前十大交易人合計(特定法人合計)部位數']
            df= df_op_top10       
            if debug&0x2!=0:
                print(lno(),df[filter])
            df_o.iloc[0]['十大 周buy call 口數']=str2int(df.iloc[0]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
            df_o.iloc[0]['十大 周buy put 口數']=str2int(df.iloc[3]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
            df_o.iloc[0]['十大 月buy call 口數']=str2int(df.iloc[1]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
            df_o.iloc[0]['十大 月buy put 口數']=str2int(df.iloc[4]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
            df_o.iloc[0]['十大 周sell call 口數']=str2int(df.iloc[0]['賣方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
            df_o.iloc[0]['十大 周sell put 口數']=str2int(df.iloc[3]['賣方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
            df_o.iloc[0]['十大 月sell call 口數']=str2int(df.iloc[1]['賣方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
            df_o.iloc[0]['十大 月sell put 口數']=str2int(df.iloc[4]['賣方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
            df_o.iloc[0]['十大 所有buy call 口數']=str2int(df.iloc[2]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
            df_o.iloc[0]['十大 所有buy put 口數']=str2int(df.iloc[5]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
            df_o.iloc[0]['十大 所有sell call 口數']=str2int(df.iloc[2]['賣方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
            df_o.iloc[0]['十大 所有sell put 口數']=str2int(df.iloc[5]['賣方前十大交易人合計(特定法人合計)部位數'].split(' ')[0])
            """
            try:
                w_top10_buycall=str2int(df.iloc[0]['買方前十大交易人合計(特定法人合計)部位數'].split('(')[1].replace(')',''))
            except:
                w_top10_buycall=0
            try:        
                w_top10_buyput=str2int(df.iloc[3]['買方前十大交易人合計(特定法人合計)部位數'].split('(')[1].replace(')',''))
            except:
                w_top10_buyput=0    
            m_top10_buycall=str2int(df.iloc[1]['買方前十大交易人合計(特定法人合計)部位數'].split('(')[1].replace(')',''))
            m_top10_buyput=str2int(df.iloc[4]['買方前十大交易人合計(特定法人合計)部位數'].split('(')[1].replace(')',''))
            """
            try:
                w_top10_buycall=str2int(df.iloc[0]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0].replace(')',''))
            except:
                w_top10_buycall=0
            try:        
                w_top10_buyput=str2int(df.iloc[3]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0].replace(')',''))
            except:
                w_top10_buyput=0    
            m_top10_buycall=str2int(df.iloc[1]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0].replace(')',''))
            m_top10_buyput=str2int(df.iloc[4]['買方前十大交易人合計(特定法人合計)部位數'].split(' ')[0].replace(')',''))
            if debug&0x4!=0:
                print(lno(),w_top10_buycall,w_top10_buyput,m_top10_buycall,m_top10_buyput)
            
        if debug&0x4!=0:
            print(lno(),"\r\n",df_o[top10_cols].iloc[0])
        list=['外資','自營商','投信']
        for key in list:
            df=df_fut_big3
            ##三大期貨 
            try:
                big_fut=df[(df['身份別'] == key)&(df['商品名稱'] == '臺股期貨')].iloc[0]['未平倉餘額多空淨額口數']
            except:
                print(lno(),df)    
            small_fut=df[(df['身份別'] == key)&(df['商品名稱'] == '小型臺指期貨')].iloc[0]['未平倉餘額多空淨額口數']
            df_o.iloc[0]['{}大小台'.format(key)]=(big_fut+small_fut/4)
            ##三大 op buy call 
            df=df_op_big3
            filter=(df['商品名稱'] == '臺指選擇權')&(df['權別'] == '買權')&(df['身份別'] == key) 
            df_o.iloc[0]['{} buy call num'.format(key)]=str2int(df[filter].iloc[0]['未平倉買方口數'])
            df_o.iloc[0]['{} buy call cash'.format(key)]=str2int(df[filter].iloc[0]['未平倉買方契約金額'])
            if debug&0x2!=0:
                print(lno(),key,df[filter].iloc[0]['未平倉買方口數'])
            if df[filter].iloc[0]['未平倉買方口數']!=0:
                price=str2int(df[filter].iloc[0]['未平倉買方契約金額'])*1000/50/str2int(df[filter].iloc[0]['未平倉買方口數'])
                if key=='外資':
                    print(lno(),'buy call cash:',df[filter].iloc[0]['未平倉買方契約金額'])
                    print(lno(),'buy call 口數',df[filter].iloc[0]['未平倉買方口數'])
                    print(lno(),'buy call avg price',price)
                    w_call_delta,m_call_delta,delta=calc_fix_delta(enddate,'買權',price,df_op_top10,debug=1,v=2)
                    
                else: 
                    delta=calc_fix_delta(enddate,'買權',price,df_op_top10)
                op_call_fut=delta*df_o.iloc[0]['{} buy call num'.format(key)]/4
                print(lno(),key,"op_call_fut",delta,df_o.iloc[0]['{} buy call num'.format(key)],op_call_fut) 
            else:    
                op_call_fut=0
            ##三大 op buy put     
            filter=(df['商品名稱'] == '臺指選擇權')&(df['權別'] == '賣權')&(df['身份別'] == key)    
            df_o.iloc[0]['{} buy put num'.format(key)]=str2int(df[filter].iloc[0]['未平倉買方口數'])   
            df_o.iloc[0]['{} buy put cash'.format(key)]=str2int(df[filter].iloc[0]['未平倉買方契約金額'])   
            if df[filter].iloc[0]['未平倉買方口數']!=0:
                price=str2int(df[filter].iloc[0]['未平倉買方契約金額'])*1000/50/str2int(df[filter].iloc[0]['未平倉買方口數'])
                if key=='外資':
                    print(lno(),'buy put cash:',df[filter].iloc[0]['未平倉買方契約金額'])
                    print(lno(),'buy put 口數',df[filter].iloc[0]['未平倉買方口數'])
                    print(lno(),'buy put avg price',price)
                    w_put_delta,m_put_delta,delta=calc_fix_delta(enddate,'賣權',price,df_op_top10,debug=1,v=2)
                else:    
                    delta=calc_fix_delta(enddate,'賣權',price,df_op_top10)
                op_put_fut=delta*df_o.iloc[0]['{} buy put num'.format(key)]/4
                print(lno(),key,"op_put_fut",delta,df_o.iloc[0]['{} buy put num'.format(key)],op_put_fut) 
            else:    
                op_put_fut=0
            df_o.iloc[0]['{} op約當大台'.format(key)]= op_call_fut+ op_put_fut  
            ##三大 op sell call
            filter=(df['商品名稱'] == '臺指選擇權')&(df['權別'] == '買權')&(df['身份別'] == key)    
            df_o.iloc[0]['{} sell call num'.format(key)]=str2int(df[filter].iloc[0]['未平倉賣方口數'])   
            df_o.iloc[0]['{} sell call cash'.format(key)]=str2int(df[filter].iloc[0]['未平倉賣方契約金額']) 
            if df[filter].iloc[0]['未平倉賣方口數']!=0:
                price=str2int(df[filter].iloc[0]['未平倉賣方契約金額'])*1000/50/str2int(df[filter].iloc[0]['未平倉賣方口數'])
                w_press,m_press=calc_pressure_support(enddate,'買權',price,df_op_top10,debug=0)
                df_o.iloc[0]['{} 周壓力'.format(key)]= w_press
                df_o.iloc[0]['{} 月壓力'.format(key)]= m_press
                #op_call_fut=delta*df_o.iloc[0]['{} buy call num'.format(key)]/4
                #print(lno(),key,"op_call_fut",delta,df_o.iloc[0]['{} buy call num'.format(key)],op_call_fut)   
            ##三大 op sell put    
            filter=(df['商品名稱'] == '臺指選擇權')&(df['權別'] == '賣權')&(df['身份別'] == key)    
            df_o.iloc[0]['{} sell put num'.format(key)]=str2int(df[filter].iloc[0]['未平倉賣方口數'])   
            df_o.iloc[0]['{} sell put cash'.format(key)]=str2int(df[filter].iloc[0]['未平倉賣方契約金額'])  
            if df[filter].iloc[0]['未平倉賣方口數']!=0:
                price=str2int(df[filter].iloc[0]['未平倉賣方契約金額'])*1000/50/str2int(df[filter].iloc[0]['未平倉賣方口數'])
                w_support,m_support=calc_pressure_support(enddate,'賣權',price,df_op_top10,debug=0)
                df_o.iloc[0]['{} 周支撐'.format(key)]= w_support
                df_o.iloc[0]['{} 月支撐'.format(key)]= m_support 
       
        df_o.iloc[0]['十大 周op約當大台']=(w_top10_buycall*w_call_delta+ w_top10_buyput*w_put_delta)/4
        df_o.iloc[0]['十大 月op約當大台']=(m_top10_buycall*m_call_delta+ m_top10_buyput*m_put_delta)/4
        df_o.iloc[0]['w call delta']=w_call_delta
        df_o.iloc[0]['m call delta']=m_call_delta
        df_o.iloc[0]['w put delta']=w_put_delta
        df_o.iloc[0]['m put delta']=m_put_delta
        
        if debug&0x2!=0:
            print(lno(),"\r\n",df_o[f_big3_cols].round(2).iloc[0])
            print(lno(),"\r\n",df_o[s_big3_cols].round(2).iloc[0])
            print(lno(),"\r\n",df_o[t_big3_cols].round(2).iloc[0])
        ##df_o['日期']=comm.date_sub2time64(enddate.strftime('%Y/%m/%d'))
        o_list=df_o.iloc[0].values.tolist()
        o_list[0]=enddate.strftime('%Y/%m/%d')
        df_o=pd.DataFrame([o_list],columns=outcols)
        df_o=df_o.replace(np.NaN,0)
        #print(lno(),o_list)
        #print(lno(),df_o)
        #存入歷史資料
        if os.path.exists(out_file): 
            df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype={'日期': 'str'})
            print(lno(),df_s)
            df_s.dropna(axis=1,how='all',inplace=True)
            #df_s.dropna(inplace=True)
            #print(lno(),df_s.iloc[0])
            #print(lno(),df_o.iloc[0])
            df_s=pd.concat([df_s,df_o],axis=0)
            #print(lno(),df_s)
            df_s['日期']=[comm.date_sub2time64(x) for x in df_s['日期'] ]    
            df_s.drop_duplicates(subset=['日期'],keep='last',inplace=True)
            df_s=df_s.sort_values(by=['日期'], ascending=False)
            df_s.to_csv(out_file,encoding='utf-8', index=False)
        else :
            df_o.to_csv(out_file,encoding='utf-8', index=False)   
##=IMPORTDATA("https://raw.githubusercontent.com/chuckai1224/final/master/day_report.csv")c
def gen_final_html():
    out_file='final/fut_day_report_fin.csv'
    if os.path.exists(out_file): 
        df1 = pd.read_csv(out_file,encoding = 'utf-8',dtype={'日期': 'str'})    
    out_file='final/day_report.csv'
    if os.path.exists(out_file): 
        df2 = pd.read_csv(out_file,encoding = 'utf-8',dtype={'日期': 'str'})
    df_o=pd.merge(df1,df2,how='left')  
    """
    df_o['十大 周op約當大台']=df_o['_十大 周op約當大台']
    df_o['十大 月op約當大台']=df_o['_十大 月op約當大台']
    df_o['w call delta']=df_o['_w call delta']
    df_o['m call delta']=df_o['_m call delta']
    df_o['w put delta']=df_o['_w put delta']
    df_o['m put delta']=df_o['_m put delta']
    df_o.drop('_十大 周op約當大台',axis=1,inplace=True)
    df_o.drop('_十大 月op約當大台',axis=1,inplace=True)
    df_o.drop('_w call delta',axis=1,inplace=True)
    df_o.drop('_m call delta',axis=1,inplace=True)
    df_o.drop('_w put delta',axis=1,inplace=True)
    df_o.drop('_m put delta',axis=1,inplace=True)
    """
    print(lno(),"teeee")
    #df_o.to_csv('final/test.csv',encoding='utf-8', index=False) 
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)
    ##=IMPORThtml("https://raw.githubusercontent.com/chuckai1224/final/master/fut_day_report_fin.html","table",1)
    df_o.to_html('final/fut_day_report_fin.html',escape=False,index=False,sparsify=True,border=2,index_names=False)
    df_o.to_csv('final/mix_report_fin.csv',encoding='utf-8', index=False)
    pd.set_option('display.max_colwidth', old_width)    
         
def down_fut_op_big3_top10_datas(startdate,enddate):
    nowdate=enddate
    print(lno(),startdate,enddate)
    while   nowdate>=startdate :
        print(lno(),nowdate)
        down_fut_op_big3_top10_data_bydate(nowdate) #download 期貨收盤價
        nowdate = nowdate - relativedelta(days=1)
    print(lno(),nowdate)
    gen_final_html()

def get_fut_op_big3_top10_data_by_date(enddate):
    filter1=['日期','期貨收盤價',#0
            '外資大小台','外資 op約當大台','外資 月支撐','外資 月壓力','外資 周支撐','外資 周壓力',#1#1
            '外資 buy call num','外資 buy call cash','外資 buy put num','外資 buy put cash',#5
            '外資 sell call num','外資 sell call cash','外資 sell put num','外資 sell put cash',#9
            '自營商大小台','自營商 op約當大台','自營商 月支撐','自營商 月壓力','自營商 周支撐','自營商 周壓力',#10
            '自營商 buy call num','自營商 buy call cash','自營商 buy put num','自營商 buy put cash',#14
            '自營商 sell call num','自營商 sell call cash','自營商 sell put num','自營商 sell put cash',#18
            '投信大小台','投信 op約當大台','投信 月支撐','投信 月壓力','投信 周支撐','投信 周壓力',#19
            '投信 buy call num','投信 buy call cash','投信 buy put num','投信 buy put cash',#23
            '投信 sell call num','投信 sell call cash','投信 sell put num','投信 sell put cash',#27
            '十大 月淨多單','十大 所有淨多單',#29
            '十大 周buy call 口數','十大 周buy put 口數',#31
            '十大 月buy call 口數','十大 月buy put 口數',#33
            '十大 周sell call 口數','十大 周sell put 口數',#35
            '十大 月sell call 口數','十大 月sell put 口數',#37
            '十大 所有buy call 口數','十大 所有buy put 口數',#39
            '十大 所有sell call 口數','十大 所有sell put 口數',#41
            '自營商(自行買賣)買賣差額',
            '自營商(避險)買賣差額',
            '投信買賣差額',
            '外資及陸資買賣差額',
            '外資自營商買賣差額',
            ]
    out_file='final/fut_day_report_fin.csv'
    if os.path.exists(out_file): 
        df_s = pd.read_csv(out_file,encoding = 'utf-8',dtype={'日期': 'str'})
        #print(lno(),df_s)
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s['日期']=[comm.date_sub2time64(x) for x in df_s['日期'] ] 
        df=df_s[(df_s.loc[:,"日期"] <= np.datetime64(enddate))].copy()
        #df1=df[]
        #print(lno(),df_s)
        return df
    return pd.DataFrame()    
    pass


def dw_major_power(enddate,download=0,debug=0xff): 
    filename='{}/dw_major_power.{}'.format(save_path,enddate.strftime('%Y%m%d'))
    check_dst_folder(save_path)
    datestr = enddate.strftime('%Y/%m/%d')    
    #url = 'https://fubon-ebrokerdj.fbs.com.tw/Z/ZG/ZGB/ZGB.djhtm'
    url = 'http://5850web.moneydj.com/Z/ZG/ZGB/ZGB.djhtm'
    print(lno(),"test")
    if download==1:
        try:
            page = requests.get(url)
        except:
            print(lno(),url)    
            raise
        if not page.ok:
            print(lno(),"Can not get data at {}".format(url))
            return 
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in page:
                file.write(chunk)
    if not os.path.exists(filename): 
        return pd.DataFrame()
    print(lno(),filename)    
    try:
        soup = BeautifulSoup(filename,'lxml')
        #[0]将返回的list改为bs4类型
        content = soup.select('#Table')[0] 
        tbl = pd.read_html(content.prettify(),header = 0)[0]    
        df = pd.read_html(filename)
        print(lno(),len(df))
        print(df[1])
    except:
        print(lno(),filename,"ng file")
        return pd.DataFrame()  
    
    pass

if __name__ == '__main__':
    #print (lno(),sys.path[0])
    #get_cur_twii_list(datetime.today())
    if len(sys.argv)==1:
        startdate=stock_comm.get_date()
        #down_fut_op_big3(startdate)
        enddate=startdate
        down_fut_op_big3_top10_datas(startdate,enddate) 
        #startdate=enddate
        #down_gen_day_datas(startdate,enddate)
        #down_fut_op_big3_top10_datas(startdate,enddate) 
    elif sys.argv[1]=='-d' :
        #print (lno(),len(sys.argv))
        if len(sys.argv)==3 :
            # 從今日往前抓一個月
            enddate=datetime.strptime(sys.argv[2],'%Y%m%d')
            down_data(enddate)  
            generate_final(enddate)  
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
    elif sys.argv[1]=='-d1' :
        try:
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        except:
            print (lno(),'func -p startdata enddate')
            raise   
        try:
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        except:
            enddate=startdate
        down_fut_op_big3_top10_datas(startdate,enddate)   
    elif sys.argv[1]=='-d2' :
        try:
            enddate=datetime.strptime(sys.argv[2],'%Y%m%d')
        except:
            print (lno(),'func -p enddate ')
            raise   
        print(lno())
        dw_major_power(enddate)
        
    elif sys.argv[1]=='-g1' :
        try:
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        except:
            print (lno(),'func -p startdata enddate')
            raise   
        list =get_fut_op_big3_top10_data_by_date(startdate)
        print(lno())
        print(list)          
    else:   
        objdatetime=datetime.strptime(sys.argv[1],'%Y%m%d')
        #down_fut_op_big3(objdatetime)
        