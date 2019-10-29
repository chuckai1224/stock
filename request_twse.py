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
#import pyecharts
#from pyecharts import Kline
#from pyecharts import Candlestick
#import webbrowser
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)
TWIIPATH='csv/twii'
TSE_KLINE_PATH='csv/tse_kline'
#TWIIURL = 'http://www.twse.com.tw/ch/trading/exchange/FMTQIK/FMTQIK2.php?STK_NO=&type=csv&'
TWIIURL = 'http://www.twse.com.tw/ch/trading/exchange/FMTQIK/FMTQIK.php?STK_NO=&type=csv&'
def check_dst_folder(dstpath):
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath)     
def get_list_from_file(fpath):
    #print lno(),fpath
    result = []
    try:
        with open(fpath) as csvfile:
            csv_files = csv.reader(csvfile)
            #for t in csv_files:
                #print t    
            datalist = []
            #print lno()
            datalist = list(csv_files)
            #print lno(),datalist
            """
            tmp_list=datalist[-1][0].split ('/')
            tmp_str="%d%0s%0s"%(int(tmp_list[0])+1911,tmp_list[1],tmp_list[2])
            #print datetime.strptime(tmp_str,'%Y%m%d'),nowdatetime
            if datetime.strptime(tmp_str,'%Y%m%d') >= nowdatetime :
                #print datalist
                return datalist
            """
            for j in datalist:
                #print lno(),len(j)
                if len(j)!=7 and len(j)!=6 :
                    continue  
                tmp_list=j[0].split ('/')
                if len(tmp_list)!=3 :
                    continue
                #print lno(),tmp_list,j  
                result.append(j)
                """  
                tmp_str="%d%0s%0s"%(int(tmp_list[0])+1911,tmp_list[1],tmp_list[2])
                print datetime.strptime(tmp_str,'%Y%m%d'),nowdatetime
                if datetime.strptime(tmp_str,'%Y%m%d') >= nowdatetime :
                #print datalist
                    return datalist    
                """    
            return result    
    except IOError as errno:
        print (' file io error')
        pass
    except:
        #print  lno(),datalist,'error'   
        print  (lno(),'error'   )
        pass
    return []    

def get_list_form_url_and_save(url,fpath):
    """        
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0'}
    request = urllib2.Request(url,headers=headers)
    response = urllib2.urlopen(request)
    cr = csv.reader(response)
    """
    download = requests.get(url)
    #print lno(),response.text
    with open(fpath, "wb") as code:
        code.write(download.content)
    decoded_content = download.content.decode('cp950')

    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    print (lno(),cr)
    datalist1=[]
    datalist = list(cr)
    for i in datalist:
        print (lno(),len(i))
        if len(i)==6 or len(i)==7:
            datalist1.append(i)
    print (lno(),datalist1)
    #print 'write file'
   
    return datalist1
def get_list_form_posturl_and_save(url,data,fpath):        
    cr=cmm.get_post_url_csv(url,data)
    
    datalist = list(cr)
    #print lno(),datalist
    #print lno(),'write file'
    with open(fpath, 'wb') as csv_outfile:
        writer = csv.writer(csv_outfile)
        for i in datalist:
            #print lno(),len(i)
            if len(i)==6 or len(i)==7:
                writer.writerow(i)
    return datalist 
def twii_to_list(csv_list):
    tolist = []
    for i in csv_list:
        #print i
        i = [value.strip().replace(',', '') for value in i]
        #print lno(),len(i)
        if len(i)!=7:
            continue
        tmp_list=i[0].split ('/')
        #print len(tmp_list),tmp_list
        #print tmp_list[0],tmp_list[1],tmp_list[2]
        #if len(tmp_list)!=3:
            #continue
        try :
            tmp_str="%d%0s%0s"%(int(tmp_list[0])+1911,tmp_list[1],tmp_list[2])
            i[0]=datetime.strptime(tmp_str,'%Y%m%d')
        except :
            #print tmp_list[0]
            #print lno(),i
            continue
            pass
        try:
            for value in (1, 2, 3, 4, 5):
                i[value] = float(i[value])
        except (IndexError, ValueError):
            #print lno(),i
            pass
        else:
            #print lno(),i   
            tolist.append(i)
    return tolist       
FMTQIK_URL='http://www.twse.com.tw/exchangeReport/FMTQIK?response=csv&date='
#http://www.twse.com.tw/fund/BFI82U?response=csv&dayDate=20190222&weekDate=20190218&monthDate=20190222&type=day
def get_twii_month_list(nowdatetime):
    twii_list=[]
    final_list=[]
    #url_get='%smyear=%d&mmon=%02d' % (FMTQIK_URL,int(nowdatetime.year), int(nowdatetime.month))
#    url_v = 'http://www.twse.com.tw/exchangeReport/FMTQIK?response=csv&date=%d/%02d/%02d'%(int(nowdatetime.year)-1911,int(nowdatetime.month),int(nowdatetime.day))
    url_get0='http://www.twse.com.tw/exchangeReport/FMTQIK?response=csv&date=%d%02d%02d'%(int(nowdatetime.year),int(nowdatetime.month),int(nowdatetime.day))
    
    url = 'http://www.twse.com.tw/ch/trading/exchange/FMTQIK/FMTQIK.php'
    data ={ 'download':'csv','query_year':'2016','query_month':'8'}
    data['query_year']=int(nowdatetime.year)
    data['query_month']=int(nowdatetime.month)
    dstpath='%s/%d%02d'%(TWIIPATH,int(nowdatetime.year), int(nowdatetime.month))
    check_dst_folder(TWIIPATH)
    twii_list=get_list_from_file(dstpath)
    #print lno(),twii_list

    if twii_list==[]:
        print (lno(),"get data")
        twii_list=get_list_form_url_and_save(url_get0,dstpath)
        #twii_list=get_list_form_posturl_and_save(url,data,dstpath)
   
    #print lno(),twii_list
    final_list=twii_to_list(twii_list)
    #print lno(),final_list
    return final_list
def getKey(item):
    #print item[0]
    return item[0]
def twii_ma(list1,cnt):
    r=0.0
    ccnt=1
    for i in list1:
        r+=i[4]
        #print i[4]
        if ccnt>=cnt :
            break
        ccnt+=1 
    result=r/cnt
    return round(result,3)
def twii_volav(list1,cnt):
    r=0.0
    ccnt=1
    for i in list1:
        r+=i[2]/100000000
        #print i[4]
        if ccnt>=cnt :
            break
        ccnt+=1 
    result=r/cnt
    return round(result,3)          
def fetch_twii(objdate,month,cur_list):
    result=[]
    tolist=[]
    sr_list=[]
    """
    get 5ma 20ma 
    get 5mv 10mv
    get preday_price, nowday_price
    get preday_vol, nowday_vol
    """
    keys = ["date","pre_pri","pre_vol","now_pri","now_vol","av5","av20","mv5","mv10","twse_bull","twse_bear","otc_bull","otc_bear"]
    s=dict.fromkeys(keys, None)
    for i in range(month):
        nowdatetime = objdate - relativedelta(months=i)
        #print nowdatetime
        tolist +=get_twii_month_list(nowdatetime)
        #print lno(),tolist
    #print lno(),tolist    
    for i in tolist:
        #print lno(),len(i),i[0],objdate
        if i[0]<=objdate:
            result.append(i)
    #print cur_list     
    if cur_list !=None :
        tmp=[]
        tmp.append(cur_list[0])
        tmp.append(0)
        tmp.append(0)
        tmp.append(cur_list[4])
        tmp.append(cur_list[5])
        tmp.append(cur_list[5]-cur_list[6])
        #print tmp
        result.append(tmp)
    #print lno(),result       
    sr_list=sorted(result, key=getKey,reverse=True)
    #print (lno(),len(sr_list))
    if sr_list[0][0]!=objdate:
        #print (lno(),sr_list[0][0],objdate)
        return None
    s['date']=sr_list[0][0]
    
    s['pre_pri']=sr_list[1][4]
    s['pre_vol']=sr_list[1][2]/100000000
    s['now_pri']=sr_list[0][4]
    s['now_vol']=sr_list[0][2]/100000000
    s['mv5']=twii_volav(sr_list,5)
    s['mv10']=twii_volav(sr_list,10)
    s['av5']=twii_ma(sr_list,5)
    s['av20']=twii_ma(sr_list,20)
    bsratio=stock_comm.get_trend_ex(s['date'])
    if bsratio!=None :
        
        s['twse_bull']=(bsratio[1]+bsratio[3])/2
        s['twse_bear']=(bsratio[2]+bsratio[5])/2
        s['otc_bull']=(bsratio[7]+bsratio[9])/2
        s['otc_bear']=(bsratio[8]+bsratio[11])/2
    #print ma5,ma20,ma5-ma20
    #print s
    return s
 
def get_cur_twii_list(date):
    url='http://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_t00.tw&json=1&delay=0&d=%s'%date.strftime('%Y%m%d')
    headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
    request = urllib2.Request(url,headers=headers)
    response = urllib2.urlopen(request)
    cur_twii_list=[]
    cur_twii_list.append(date)
    tmp=response.read().split (',')
    for i in tmp:
        tmp1=i.replace('"',"").split(':')
        if tmp1[0]=='o' :
            cur_twii_list.append(float(filter(lambda ch: ch in '0123456789.',tmp1[1])))
            #print tmp1[0], filter(lambda ch: ch in '0123456789.',tmp1[1])
        if tmp1[0]=='l' :
            cur_twii_list.append(float(filter(lambda ch: ch in '0123456789.',tmp1[1])))
        if tmp1[0]=='h' :
            cur_twii_list.append(float(filter(lambda ch: ch in '0123456789.',tmp1[1])))
        if tmp1[0]=='v' :
            cur_twii_list.append(float(filter(lambda ch: ch in '0123456789.',tmp1[1])))
        if tmp1[0]=='z' :
            cur_twii_list.append(float(filter(lambda ch: ch in '0123456789.',tmp1[1])))
        if tmp1[0]=='y' :
            cur_twii_list.append(float(filter(lambda ch: ch in '0123456789.',tmp1[1])))
            #print tmp1[0],tmp1[1]
    return cur_twii_list

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
                
    
def download_twii(startdate,enddate):
    check_dst_folder(TWIIPATH)
    nowdatetime = enddate
    month=0
    while   nowdatetime>=startdate :
        print (lno(),nowdatetime)
        dstpath='%s/%d%02d'%(TWIIPATH,int(nowdatetime.year), int(nowdatetime.month))
        url_get0='http://www.twse.com.tw/exchangeReport/FMTQIK?response=csv&date=%d%02d%02d'%(int(nowdatetime.year),int(nowdatetime.month),int(nowdatetime.day))
        download_file(url_get0,dstpath)
        time.sleep(2)
        month=month+1
        nowdatetime = enddate - relativedelta(months=month)
    return []
 
"""        
def get_data(self):
    endpoint = 'http://mis.twse.com.tw/stock/api/getStockInfo.jsp'
    timestamp = int(time.time() * 1000 + 1000000)
    channels = '|'.join('tse_{}.tw'.format(target) for target in targets)
    self.query_url = '{}?_={}&ex_ch={}'.format(endpoint, timestamp, channels)
    try:
        # Get original page to get session
        req = requests.session()
        req.get('http://mis.twse.com.tw/stock/index.jsp',
                headers={'Accept-Language': 'zh-TW'})

        response = req.get(self.query_url)
        content = json.loads(response.text)
    except Exception as err:
        print(err)
        data = []
    else:
        data = content['msgArray']

    return data    
"""
def show_twii(objdatetime):
    fin=[]
    tt=0
    for j in range(30):
        nowdatetime = objdatetime - relativedelta(days=j)
        res=fetch_twii(nowdatetime,4,None)
        if res!=None:
            #print res['date'].date(),res['av5'],res['av20']
            fin.append(res)
            tt+=1
            if tt>=20:
                break
        else :
            print (lno(), nowdatetime       )
    print ('%s'%(u'    日期,  指  數 ,成交量,  5日均,   20日均, 雙均差, 多頭,空頭, 多頭,空頭,       ,'))            
    for j in range(len(fin)-1):
        print ('%4d%02d%02d,'%(fin[j]['date'].year,fin[j]['date'].month,fin[j]['date'].day),end='') 
        print ('%8.2f ,%6.0f,'%(fin[j]['now_pri'],fin[j]['now_vol']),end='')    
        if fin[j]['av5']>=fin[j+1]['av5']:
            print ('%6s,'%("5ma up"),end='')
        else :  
            print ('%6s,'%("5ma dn"),end='')
        if fin[j]['av20']>=fin[j+1]['av20']:
            print ('%8s,'%("20ma up"),end='')
        else :  
            print ('%8s,'%("20ma dn"),end='')   
        print ("均差:%6d,"%(fin[j]['av5']-fin[j]['av20']),end='')
        print ("上市(%4d,%4d),"%(fin[j]['twse_bull'],fin[j]['twse_bear']),end='')
        print ("上櫃(%4d,%4d),"%(fin[j]['otc_bull'],fin[j]['otc_bear']),end='')
        if fin[j]['now_pri']>=(fin[j]['pre_pri']*1.012) and fin[j]['now_vol']>=(fin[j]['pre_vol']*1.3):
            print ("%6s"%(u'起漲日'))
        elif fin[j]['now_pri']<=fin[j]['pre_pri'] and fin[j]['now_vol']>(fin[j]['pre_vol']):
            print ("%6s"%(u'出貨日'))
        elif fin[j]['now_pri']>=(fin[j]['pre_pri']*1.006) and fin[j]['now_vol']>=((fin[j]['mv5']+fin[j]['mv10'])/2):
            print ("%6s"%(u'多頭k'))
        else:
            print ("%6s"%("   "))
TSE_KLINE_PATH='csv/tse_kline'                
def download_file(url, filename):
    ''' Downloads file from the url and save it as filename '''
    # check if file already exists
    print('Downloading File')
    response = requests.get(url)
    # Check if the response is ok (200)
    if response.status_code == 200:
        # Open file and write the content
        #print(len(response.content))
        if len(response.content)<10:
            return 0
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in response:
                file.write(chunk)
        return 1
    return 0    
                
def down_twse_3big(startdate,enddate):
    result=[]
    sr_list=[]
    dst_folder='csv/big3'
    check_dst_folder(dst_folder)
    nowdatetime = enddate
    day=0
    while   nowdatetime>=startdate :
        print (lno(),nowdatetime)
        dstpath='%s/%d%02d%02d.csv'%(dst_folder,int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
        url='http://www.twse.com.tw/fund/BFI82U?response=csv&dayDate=%d%02d%02d&type=day'%(int(nowdatetime.year),int(nowdatetime.month),int(nowdatetime.day))
        download_file(url,dstpath)
        """
        if os.path.exists(dstpath):   
            df = pd.read_csv(dstpath,encoding = 'big5',skiprows=1)
            df.dropna(axis=1,how='all',inplace=True)
            df.dropna(inplace=True)
            print(lno(),len(df),df)
            #產生 日期,
        """    
            
        time.sleep(3)
        day=day+1
        nowdatetime = enddate - relativedelta(days=day)
def generate_twse_3big(startdate,enddate):
    out_file='csv/big3/big3_data.csv'
    dst_folder='csv/big3'
    check_dst_folder(dst_folder)
    nowdatetime = enddate
    day=0
    res=[]
    while   nowdatetime>=startdate :
        print (lno(),nowdatetime)
        nowdate='%d%02d%02d'%(int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
        dstpath='%s/%s.csv'%(dst_folder,nowdate)

        if os.path.exists(dstpath):   
            df = pd.read_csv(dstpath,encoding = 'big5',skiprows=1)
            df.dropna(axis=1,how='all',inplace=True)
            df.dropna(inplace=True)
            if len(df)==6:
                tmp=[]
                date_str='%d-%02d-%02d'%(int(nowdatetime.year), int(nowdatetime.month),int(nowdatetime.day))
                tmp.append(date_str)
                for i in range(0, len(df)):
                    tmp.append(df.at[i,'買進金額'])
                    tmp.append(df.at[i,'賣出金額'])
                    tmp.append(df.at[i,'買賣差額'])
                    
                #print(lno(),tmp)
                res.append(tmp)
            #產生 日期,
        day=day+1
        nowdatetime = enddate - relativedelta(days=day)        
    labels = ['date','自營商buy', '自營商sell', '自營商total', '自營商避險buy', '自營商避險sell', '自營商避險total','投信buy', '投信sell', '投信total', \
            '外資buy', '外資sell', '外資total','外資自營商buy', '外資自營商sell', '外資自營商total','總buy', '總sell', '總total',]

    res_df = pd.DataFrame.from_records(res, columns=labels)   
    #print (lno(),res_df)    
    if os.path.exists(out_file): 
        df_s = pd.read_csv(out_file,encoding = 'utf-8')
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        print(lno(),df_s['date'].dtype)
        print(lno(),res_df['date'].dtype)
        df_s=df_s.append(res_df,ignore_index=True)
        print(lno(),df_s[['date','自營商total']])
        df_s.drop_duplicates(subset=['date'],keep='first',inplace=True)
        print(lno(),df_s[['date','外資total','投信total','自營商total']])
        df_s.to_csv(out_file,encoding='utf-8', index=False)
        
    else :
        res_df.to_csv(out_file,encoding='utf-8', index=False)
     
        
if __name__ == '__main__':
    print (lno(),sys.path[0])
    #get_cur_twii_list(datetime.today())
    if len(sys.argv)==1:
        print('need to add')
        #get_list_form_url_and_save(url,dstpath)
        #show_twii(nowdatetime)
    elif sys.argv[1]=='-d' :
        print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            # 從今日往前抓一個月
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            down_twse_3big(startdate,enddate)  
        else :
              print (lno(),'func -p startdata enddate') 
    elif sys.argv[1]=='-p' :
        print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            #參數2:開始日期  參數3:開始日期
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            generate_twse_3big(startdate,enddate)  
            

        else :
            print (lno(),'func -p startdata enddate')  
    elif sys.argv[1]=='kline' :
        print (lno(),len(sys.argv))
        if len(sys.argv)==4 :
            # 從今日往前抓一個月
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
            big3_kline_data(startdate,enddate)

        else :
            #參數2:開始日期  參數3:開始日期
            print(lno(),'fun -k startdate enddate')
            
            
    else:   
        objdatetime=datetime.strptime(sys.argv[1],'%Y%m%d')
        show_twii(objdatetime)
        