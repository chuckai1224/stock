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
import big3_fut_op
import requests
import inspect
from inspect import currentframe, getframeinfo
import pandas as pd
import numpy as np
import twse_big3
import taiwan_dollar
import op
import powerinx
import stock_bs_analy
import fut
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
        time.sleep(3)
        month=month+1
        nowdatetime = enddate - relativedelta(months=month)
    generate_twii(startdate,enddate)    
    return []
 

def twdate2datetime64(x):
    tmp_list=x.split ('/')
    try :
        tmp_str="%d%0s%0s"%(int(tmp_list[0])+1911,tmp_list[1],tmp_list[2])
        fin=datetime.strptime(tmp_str,'%Y%m%d')
    except :
    #print tmp_list[0]
        print (lno(),tmp_list[0])
        pass
    return np.datetime64(fin)
def date_sub2time64(x):
    fin=datetime.strptime(x,'%Y-%m-%d')
    return np.datetime64(fin) 
def date_slash2time64(x):
    fin=datetime.strptime(x,'%Y/%m/%d')
    return np.datetime64(fin)      
def generate_twii(startdate,enddate,debug=1):
    out_file='csv/twii/twii_data.csv'
    dst_folder='csv/twii'
    check_dst_folder(dst_folder)
    nowdatetime = enddate
    month=0
    while   nowdatetime>=startdate :
        print (lno(),nowdatetime)
        dstpath='%s/%d%02d'%(TWIIPATH,int(nowdatetime.year), int(nowdatetime.month))
        if os.path.exists(dstpath):   
            df = pd.read_csv(dstpath,encoding = 'big5',skiprows=1)
            df.dropna(axis=1,how='all',inplace=True)
            df.dropna(inplace=True)
            df['日期']=[twdate2datetime64(x) for x in df['日期'] ]
            df['成交股數']=[int(x.strip().replace(',', '')) for x in df['成交股數'] ]
            df['成交金額']=[int(x.strip().replace(',', '')) for x in df['成交金額'] ]
            df['發行量加權股價指數']=[float(x.strip().replace(',', '')) for x in df['發行量加權股價指數'] ]
            if debug==1:
                print(lno(),len(df),df)

            if os.path.exists(out_file): 
                df_s = pd.read_csv(out_file,encoding = 'utf-8')
                df_s.dropna(axis=1,how='all',inplace=True)
                df_s.dropna(inplace=True)
                if debug==1:
                    print(lno(),df_s.dtypes)
                df_s['日期']=[date_sub2time64(x) for x in df_s['日期'] ]    
                df_s=df_s.append(df,ignore_index=True)
                
                df_s.drop_duplicates(subset=['日期'],keep='first',inplace=True)
                if debug==1:
                    print(lno(),df_s.dtypes)
               
                df_s=df_s.sort_values(by=['日期'], ascending=False)
                df_s.to_csv(out_file,encoding='utf-8', index=False)
            else :
                df.to_csv(out_file,encoding='utf-8', index=False)    

        month=month+1
        nowdatetime = enddate - relativedelta(months=month)
    
def show_twii(objdatetime):
    fin=[]
    tt=0
    for j in range(60):
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
    print ('上市多頭:',powerinx.get_tse_power_inx(fin[0]['date'],'up'))
    print ('上櫃多頭:',powerinx.get_otc_power_inx(fin[0]['date'],'up'))
    print ('上市空頭:',powerinx.get_tse_power_inx(fin[0]['date'],'down'))
    print ('上櫃空頭:',powerinx.get_otc_power_inx(fin[0]['date'],'down'))        
    print ('%s'%(u'    日期,  指  數 ,成交量,  5日均,   20日均, 雙均差, 多頭,空頭, 多頭,空頭,       ,'))            
    for j in range(len(fin)-1):
        foreign=twse_big3.get_foreign_investment(fin[j]['date'])
        #print(lno(),fin[j]['date'],foreign)
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
        print ("外資:%4d億,"%(twse_big3.get_foreign_investment(fin[j]['date'])),end='')
        print ("匯率:%6.3f,"%(taiwan_dollar.get_taiwan_dollor(fin[j]['date'])),end='')
        if fin[j]['now_pri']>=(fin[j]['pre_pri']*1.012) and fin[j]['now_vol']>=(fin[j]['pre_vol']*1.3):
            print ("%6s"%(u'起漲日'))
        elif fin[j]['now_pri']<=fin[j]['pre_pri'] and fin[j]['now_vol']>(fin[j]['pre_vol']):
            print ("%6s"%(u'出貨日'))
        elif fin[j]['now_pri']>=(fin[j]['pre_pri']*1.006) and fin[j]['now_vol']>=((fin[j]['mv5']+fin[j]['mv10'])/2):
            print ("%6s"%(u'多頭k'))
        else:
            print ("%6s"%("   "))
    #print(lno(),fin)
    out_list=[]
    out_list1=[]
    out_list2=[]
    for j in range(len(fin)-1):
        tmp=[]
        tmp1=[]
        tmp2=[]
        tmp.append('%4d%02d%02d'%(fin[j]['date'].year,fin[j]['date'].month,fin[j]['date'].day))
        tmp1.append('%4d%02d%02d'%(fin[j]['date'].year,fin[j]['date'].month,fin[j]['date'].day))
        tmp2.append('%4d%02d%02d'%(fin[j]['date'].year,fin[j]['date'].month,fin[j]['date'].day))
        tmp.append('%8.2f'%(fin[j]['now_pri']))
        tmp2.append('%8.2f'%(fin[j]['now_pri']))
        tmp.append('%6.0f'%(fin[j]['now_vol']))
        if fin[j]['av5']>=fin[j+1]['av5']:
            tmp.append ('%6s'%("5ma往上"))
        else :  
            tmp.append ('%6s'%("5ma往下"))
        if fin[j]['av20']>=fin[j+1]['av20']:
            tmp.append ('%8s'%("20ma往上"))
        else :  
            tmp.append ('%8s'%("20ma往下"))
        tmp.append ("%6d"%(fin[j]['av5']-fin[j]['av20']))        
        
        foreign=twse_big3.get_foreign_investment(fin[j]['date'])

        
        tmp.append ("%4d-%4d"%(fin[j]['twse_bull'],fin[j]['twse_bear']))
        tmp.append ("%4d-%4d"%(fin[j]['otc_bull'],fin[j]['otc_bear']))
        tmp.append ("%4d億"%(twse_big3.get_foreign_investment(fin[j]['date'])))
        tmp.append ("%5.2f"%(op.get_op_ratio(fin[j]['date'])))
        tmp.append ("%6.3f"%(taiwan_dollar.get_taiwan_dollor(fin[j]['date'])))
        if fin[j]['now_pri']>=(fin[j]['pre_pri']*1.012) and fin[j]['now_vol']>=(fin[j]['pre_vol']*1.3):
            tmp.append ("%6s"%(u'起漲日'))
        elif fin[j]['now_pri']<=fin[j]['pre_pri'] and fin[j]['now_vol']>(fin[j]['pre_vol']):
            tmp.append ("%6s"%(u'出貨日'))
        elif fin[j]['now_pri']>=(fin[j]['pre_pri']*1.006) and fin[j]['now_vol']>=((fin[j]['mv5']+fin[j]['mv10'])/2):
            tmp.append ("%6s"%(u'多頭k'))
        else:
            tmp.append ("%6s"%("   "))
        tmp1.append  (powerinx.get_tse_power_inx(fin[j]['date'],'up'))
        tmp1.append  (powerinx.get_otc_power_inx(fin[j]['date'],'up'))
        tmp1.append  (powerinx.get_tse_power_inx(fin[j]['date'],'down'))
        tmp1.append  (powerinx.get_otc_power_inx(fin[j]['date'],'down'))  
        
        opOI=op.get_opData(fin[j]['date'])
        #print(lno(),opOI,len(opOI))
        tmp2.append ("%6d %6d(%6d)"%(opOI[0][0],opOI[0][1],opOI[0][2]))
        tmp2.append ("%6d %6d(%6d)"%(opOI[1][0],opOI[1][1],opOI[1][2]))
        tmp2.append ("%6d %6d(%6d)"%(opOI[4][0],opOI[4][1],opOI[4][2]))
        tmp2.append ("%6d %6d(%6d)"%(opOI[5][0],opOI[5][1],opOI[5][2]))
        tmp2.append ("%6d %6d(%6d)"%(opOI[2][0],opOI[2][1],opOI[2][2]))
        tmp2.append ("%6d %6d(%6d)"%(opOI[6][0],opOI[6][1],opOI[6][2]))

        
        out_list.append(tmp)
        out_list1.append(tmp1)
        out_list2.append(tmp2)
    labels = ['日期','指數', '量', '5ma趨勢','20ma趨勢', '均差', '上市多空家數','上櫃多空家數','外資買超','op OI','匯率','特殊信號','多頭家數','空頭家數']
    
    df = pd.DataFrame.from_records(out_list, columns=labels)
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)
    #df.to_html('files.html',escape=False,index=False,sparsify=True,border=0,index_names=False,header=False)
    df.to_html('技術分析.html',escape=False,index=False,sparsify=True,border=2,index_names=False)
    pd.set_option('display.max_colwidth', old_width)
    labels = ['日期','上市強勢主流','上櫃強勢主流','上市弱勢主流','上櫃弱勢主流']
    df1 = pd.DataFrame.from_records(out_list1, columns=labels)
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)
    #df.to_html('files.html',escape=False,index=False,sparsify=True,border=0,index_names=False,header=False)
    df1.to_html('強勢族群.html',escape=False,index=False,sparsify=True,border=2,index_names=False)
    pd.set_option('display.max_colwidth', old_width)
    labels = ['日期','指數','買權留倉最大量','買權留倉次量','賣權留倉最大量','賣權留倉次量','買權留倉增加最多','賣權留倉增加最多']
    df2 = pd.DataFrame.from_records(out_list2, columns=labels)
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)
    #df.to_html('files.html',escape=False,index=False,sparsify=True,border=0,index_names=False,header=False)
    df2.to_html('選擇權.html',escape=False,index=False,sparsify=True,border=2,index_names=False)
    pd.set_option('display.max_colwidth', old_width)

def trend2str(x):
    str='%.2f'%(x)
    if (x>0):
        return '多 :'+str
    return '空:'+str    
def op_cv1(x):
    #print(lno(),x)
    tmp=x.strip().split(' ')[0]
    if type(tmp)==str:
        #print(lno(),tmp)
        return float(tmp)
    return np.nan
def show_twii_v1_org(objdatetime,debug=0):
    df=get_days_twii_fin(objdatetime,20)
    if len(df)==0:
        return 
    if debug==1:
        print(lno(),df)
        print(lno(),df.columns)
    
    startdate=df.iloc[-1]['日期']
    enddate=df.iloc[0]['日期']
    #print(lno(),type(startdate),startdate,enddate)
    #print(lno(),type(startdate.to_pydatetime()),startdate.to_pydatetime(),enddate)
    df2=stock_bs_analy.get_stock_bs_bydate(startdate.to_pydatetime(),enddate.to_pydatetime())
    
    df2['日期']=[date_sub2time64(x) for x in df2['日期'] ]
    #print(lno(),df2)
    df=pd.merge(df,df2)
    #print(lno(),df1)
    df1=df[['日期','指數']].copy()
    
    df1['量']=df['量'].astype('int')    
    df1['5ma趨勢']=[trend2str(x) for x in df['5ma趨勢'] ]       
    df1['20ma趨勢']=[trend2str(x) for x in df['20ma趨勢'] ]       
    df1['均差']=df['均差'].astype('int')    
    #df1['上市多頭家數']=df1['上市多頭家數'].astype('str')    
    #df1['上市空頭家數']=df1['上市空頭家數'].astype('str')    
    df1['上市多空']=df['上市多頭家數'].astype('int').astype('str')+ '/'+ df['上市空頭家數'].astype('int').astype('str')
    df1['上櫃多空']=df['上櫃多頭家數'].astype('int').astype('str')+ '/'+ df['上櫃空頭家數'].astype('int').astype('str')
    df1['上市買賣力']=(df['tse_buy']/df['tse_sell']*100 ).astype('int').astype('str')+ '(' +df['tse_buy'].astype('int').astype('str')+ '/'+ df['tse_sell'].astype('int').astype('str')+')'
    df1['上櫃買賣力']=(df['otc_buy']/df['otc_sell']*100 ).astype('int').astype('str')+ '(' +df['otc_buy'].astype('int').astype('str')+ '/'+ df['otc_sell'].astype('int').astype('str')+')'
    df_3big=twse_big3.get_big3_df()
    df_3big['日期']=[date_sub2time64(x) for x in df_3big['日期'] ]
    #print(lno(),df1.head(5))
    #print(lno(),df_3big)
    df1=pd.merge(df1,df_3big)
    #print(lno(),df1.head(5))
    #raise
    df2=big3_fut_op.get_fut_op_big3_dfs_bydate(comm.time64_DateTime(df1.iloc[-1]['日期']),comm.time64_DateTime(df1.iloc[0]['日期']))
    
    df2['外資期貨']=df2['外期貨']+df2['外選擇權']
    
    df2['投信期貨']=df2['投期貨']+df2['投選擇權']
    
    df2['自營商期貨']=df2['自期貨']+df2['自選擇權']
    df2.drop('外期貨', axis=1, inplace = True)
    df2.drop('外選擇權', axis=1, inplace = True)
    df2.drop('投期貨', axis=1, inplace = True)
    df2.drop('投選擇權', axis=1, inplace = True)
    df2.drop('自期貨', axis=1, inplace = True)
    df2.drop('自選擇權', axis=1, inplace = True)
    #print(lno(),df2.head(5))
    #print(lno(),df1.head(5))
    df1=pd.merge(df1,df2)
    #df1['外資(億)']=df1['外資'].astype(str)
    #df1['投信(億)']=df1['投信'].astype(str)
    #df1['自營商(億)']=df1['自營商'].astype(str)
       
    df1['外期貨']=df1['外資期貨'].astype(int).astype(str)
    df1['投期貨']=df1['投信期貨'].astype(int).astype(str)
    df1['自期貨']=df1['自營商期貨'].astype(int).astype(str)
    df1.drop('外資期貨', axis=1, inplace = True)
    df1.drop('投信期貨', axis=1, inplace = True)
    df1.drop('自營商期貨', axis=1, inplace = True)
    #df1.drop('外資', axis=1, inplace = True)
    #df1.drop('投信', axis=1, inplace = True)
    #df1.drop('自營商', axis=1, inplace = True)
    
    #raise 
    df_big8=twse_big3.get_big8_df()
    df_big8['日期']=[date_slash2time64(x) for x in df_big8['日期'] ]
    #print(lno(),df1.head(5))
    #print(lno(),df_big8.head(5))
    #raise 
    df1=pd.merge(df1,df_big8,how='left')
    #print(lno(),df1.head(5))
    #raise
    #df1['外資買超']=df['外資買超']
    df1['op OI']=df['op OI']
    df1['匯率']=df['匯率']
    df1['特殊信號']=df['特殊信號']
    df1['多頭家數']=df['多頭家數']
    df1['空頭家數']=df['空頭家數']

    if debug==1:
        print(lno(),df1)
     
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)

    check_dst_folder('day_report/%s'%(objdatetime.strftime('%Y%m%d')))
    filen='day_report/%s/%s_技術分析.html'%(objdatetime.strftime('%Y%m%d'),objdatetime.strftime('%Y%m%d'))
    df1.to_html(filen,escape=False,index=False,sparsify=True,border=2,index_names=False)
    pd.set_option('display.max_colwidth', old_width)   

    df1=df[ ['日期','上市強勢主流','上櫃強勢主流','上市弱勢主流','上櫃弱勢主流']].copy()
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)
    filen='day_report/%s/%s_強勢族群.html'%(objdatetime.strftime('%Y%m%d'),objdatetime.strftime('%Y%m%d'))
    df1.to_html(filen,escape=False,index=False,sparsify=True,border=2,index_names=False)
    pd.set_option('display.max_colwidth', old_width)   
    
    df1=df[ ['日期','指數','買權留倉最大量','買權留倉次量','賣權留倉最大量','賣權留倉次量','買權留倉增加最多','賣權留倉增加最多']].copy()
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)
    filen='day_report/%s/%s_選擇權.html'%(objdatetime.strftime('%Y%m%d'),objdatetime.strftime('%Y%m%d'))
    df1.to_html(filen,escape=False,index=False,sparsify=True,border=2,index_names=False)
    pd.set_option('display.max_colwidth', old_width)   
    
    
    df1=df[['日期','指數']].copy()
    df_op=op.get_op_ratio_df()
    df_op['日期']=[date_slash2time64(x) for x in df_op['日期'] ]
    df1=pd.merge(df1,df_op)
    df1.columns=['日期','指數','P_VOL','C_VOL','PC_V_RATIO','P_OI','C_OI','PC_OI_RATIO','P+C','P-C']
    df_op2=op.get_week_month_op_df(df1.iloc[-1]['日期'],df1.iloc[0]['日期'])
    df_op2['日期']=[date_sub2time64(x) for x in df_op2['日期'] ]
    df1=pd.merge(df1,df_op2)
    df2=df1[['日期','PC_OI_RATIO','PC_V_RATIO']].copy()
    df2['加']=df1['PC_OI_RATIO']+df1['PC_V_RATIO']
    df2['月OI_RATIO']=df1['月PC_OI_RATIO']
    df2['月VOL_RATIO']=df1['月PC_VOL_RATIO']
    df2['月差']=df1['月PC_OI_RATIO']-df1['月PC_VOL_RATIO']
    df2['月加']=df1['月PC_OI_RATIO']+df1['月PC_VOL_RATIO']
    df2['次月OI_RATIO']=df1['次月PC_OI_RATIO']
    df2['次月VOL_RATIO']=df1['次月PC_VOL_RATIO']
    df2['次月加']=df1['次月PC_OI_RATIO']+df1['次月PC_VOL_RATIO']
    df2['指數']=df1['指數']
    df2['周OI_RATIO']=df1['周PC_OI_RATIO']
    df2['周VOL_RATIO']=df1['周PC_VOL_RATIO']
    df2['周差']=df1['周PC_OI_RATIO']-df1['周PC_VOL_RATIO']
    df2['漲跌點']=df2['指數']-df2['指數'].shift(-1)
    #print(lno(),df1)
    old_width = pd.get_option('display.max_colwidth')
    pd.set_option('display.max_colwidth', -1)
    filen='day_report/{}/{}_選擇權_v1.html'.format(objdatetime.strftime('%Y%m%d'),objdatetime.strftime('%Y%m%d'))
    df2.to_html(filen,escape=False,index=False,sparsify=True,border=2,index_names=False)
    pd.set_option('display.max_colwidth', old_width)   
    
def show_twii_v1(objdatetime,debug=0):
    startdate=datetime.strptime('20190801','%Y%m%d')
    df=get_days_twii_fin_v1(startdate,objdatetime)
    if len(df)==0:
        return 
    if debug==1:
        print(lno(),df)
        print(lno(),df.columns)
    
    startdate=df.iloc[-1]['日期']
    enddate=df.iloc[0]['日期']
    #print(lno(),type(startdate),startdate,enddate)
    #print(lno(),type(startdate.to_pydatetime()),startdate.to_pydatetime(),enddate)
    df2=stock_bs_analy.get_stock_bs_bydate(startdate.to_pydatetime(),enddate.to_pydatetime())
    #print(lno(),df2)
    if len(df2)!=0:
        df2['日期']=[date_sub2time64(x) for x in df2['日期'] ]
        df=pd.merge(df,df2)
    df_big8=twse_big3.get_big8_df()
    if len(df_big8)!=0:
        df_big8['日期']=[date_slash2time64(x) for x in df_big8['日期'] ]
        df=pd.merge(df,df_big8,how='left')
    
    df['量']=df['量'].astype('int')    
    df['5ma趨勢']=[trend2str(x) for x in df['5ma趨勢'] ]       
    df['20ma趨勢']=[trend2str(x) for x in df['20ma趨勢'] ]       
    df['均差']=df['均差'].astype('int')    
    			
    df['買權留倉最大量']=[op_cv1(x) for x in df['買權留倉最大量'] ]   
    df['賣權留倉最大量']=[op_cv1(x) for x in df['賣權留倉最大量'] ]   
    df['買權留倉增加最多']=[op_cv1(x) for x in df['買權留倉增加最多'] ]   
    df['賣權留倉增加最多']=[op_cv1(x) for x in df['賣權留倉增加最多'] ]    
    df.to_csv('final/day_report.csv',encoding='utf-8', index=False)
    if debug==1:
        print(lno(),df)
    fut.gen_final_html()    
    

def get_stock_tse_df(stock_id,df):
    df1=df[(df.loc[:,"stock_id"] == stock_id) ].sort_values(by='date', ascending=True)
    
    df1=df1.replace('--',np.NaN)
    df1=df1.replace('---',np.NaN)
    df1=df1.replace('NaN',np.NaN)
    df1=df1.fillna(method='ffill') 
    try:
        df1['close'].astype('float64')
    except:
        print(lno(),df1.tail())
    #print(lno(),df1)
    df1=df1.dropna(how='any',axis=0)
    df1=df1.tail(21)    
    df1.reset_index(inplace=True)    
    #df1=df1.replace('--',np.NaN)
    #df1=df1.fillna(method='ffill') 
    #print (lno(),df1)
    return df1
def get_stock_long_short_list(_list,date,debug=0,ver=1):     
    long1=0
    short1=0
    long2=0
    short2=0
    long3=0
    short3=0
    ll=0
    ss=0
    if ver==1:
        stk=comm.stock_data()
    for i in  _list:    
        if len(i)!=4:
            continue
        #print(i)
        if ver==1:
            _df=stk.get_df_by_date_day(i,date,21*2)
            df_stk=_df.tail(21).reset_index(drop=True)
            #print(lno(),df_stk)
        else:
            _df=comm.get_stock_df(i)
            df_stk=comm.get_df_bydate_nums(_df,21,date)
        if debug==3:
            print(lno(),i,len(df_stk))
        if len(df_stk)!=21 :
            continue
        if debug==2:
            print(lno(),i,len(df_stk))
        ma_list = [5,10,20]
        #print(lno(),df_stk.head())
        for ma in ma_list:
            df_stk['MA_' + str(ma)] = df_stk['close'].rolling(window=ma,center=False).mean()
        if debug==2:
            print(lno(),df_stk)
            print(lno(),df_stk.iloc[-1]['MA_5'],df_stk.iloc[-1]['MA_10'],df_stk.iloc[-1]['MA_20'])
        try:    
            if df_stk.iloc[-1]['close']==df_stk['close'].max():
                ll=ll+1
        except :
            print(lno(),df_stk.iloc[-1]['close'],df_stk['close'].max())
        if df_stk.iloc[-1]['close']==df_stk['close'].min():
            ss=ss+1
        ma5=df_stk.iloc[-1]['MA_5']
        ma10=df_stk.iloc[-1]['MA_10']
        ma20=df_stk.iloc[-1]['MA_20']
        pma5=df_stk.iloc[-2]['MA_5']
        pma10=df_stk.iloc[-2]['MA_10']
        pma20=df_stk.iloc[-2]['MA_20']
        if ma5>ma10 and ma10>ma20 :
            long1+=1
        elif ma5<ma10 and ma10<ma20 :
            short1+=1
        diff=ma5-ma20
        pdiff=pma5-pma20
        if diff >0 and diff >pdiff :  ##多頭
            long2+=1
        elif diff >0 :    ##多頭背離 
            short3+=1
        elif  diff <0 and diff <pdiff :
            short2+=1
        elif diff <0 :
            long3+=1
    if debug==2:
        print(lno(),long1,short1)        
        print(lno(),long2,short2)
    return  long1,short1,long2,short2,ll,ss   
def get_day_twii_fin(seldate,debug=1):
    fin_file='csv/twii/twii_data_fin.csv'
    if os.path.exists(fin_file): 
        if debug==1:
            print(lno(),"test")
        df_s = pd.read_csv(fin_file,encoding = 'utf-8')
        #df_s.dropna(axis=1,how='all',inplace=True)
        #df_s.dropna(inplace=True)
        if debug==1:
            print(lno(),"test")
        df_s['日期']=[date_sub2time64(x) for x in df_s['日期'] ]
        if debug==1:
            print(lno(),"test")
        df=df_s[(df_s.loc[:,"日期"] == np.datetime64(seldate))]
        if debug==1:
            print(lno(),"test")
        if len(df)==1:
            if debug==1:
                print(lno(),"test")
            return df
        else :
            return []
    else :
        return []
def get_days_twii_fin(seldate,cnt,debug=1):
    fin_file='csv/twii/twii_data_fin.csv'
    if os.path.exists(fin_file): 
        if debug==1:
            print(lno(),"test")
        df_s = pd.read_csv(fin_file,encoding = 'utf-8')
        #df_s.dropna(axis=1,how='all',inplace=True)
        #df_s.dropna(inplace=True)
        df_s['日期']=[date_sub2time64(x) for x in df_s['日期'] ]
        
        df=df_s[(df_s.loc[:,"日期"] <= np.datetime64(seldate))]
        if debug==1:
            print(lno(),df.iloc[0]['日期'] ,np.datetime64(seldate))
        if df.iloc[0]['日期'] == np.datetime64(seldate):
            if debug==2:
                print(lno(),"test")
            return df.head(cnt)
        else :
            return []
    else :
        return [] 
     
def get_days_twii_fin_v1(startdate,enddate,debug=1):
    fin_file='csv/twii/twii_data_fin.csv'
    if os.path.exists(fin_file): 
        if debug==1:
            print(lno(),"test")
        df_s = pd.read_csv(fin_file,encoding = 'utf-8')
        #df_s.dropna(axis=1,how='all',inplace=True)
        #df_s.dropna(inplace=True)
        df_s['日期']=[date_sub2time64(x) for x in df_s['日期'] ]
        
        df=df_s[(df_s.loc[:,"日期"] <= np.datetime64(enddate))&(df_s.loc[:,"日期"] >= np.datetime64(startdate))]
        if len(df)!=0:
            return df
        
            
    else :
        return pd.DataFrame()
## TODO regen data for fix bug        
def check_twii_fin(seldata,debug=1,regen=0):
    fin_file='csv/twii/twii_data_fin.csv'
    df_fin_=get_day_twii_fin(seldata)
    if debug==1:
        print(lno(),len(df_fin_))
    if len(df_fin_) != 0 and regen==0 :
        print (lno(),df_fin_)
        return df_fin_
    if debug==1:
        print(lno(),len(df_fin_))   
    out_file='csv/twii/twii_data.csv'
    df = pd.read_csv(out_file,encoding = 'utf-8')
    df.dropna(axis=1,how='all',inplace=True)
    df.dropna(inplace=True)
    print(lno(),debug)
    df['日期']=[date_sub2time64(x) for x in df['日期'] ]
    print(lno(),debug)
    if len(df[(df.loc[:,"日期"] == np.datetime64(seldata))])==0:
        print(lno(), seldata,"no data")
        #print(lno(), df)
        return []
    df=df[(df.loc[:,"日期"] <= np.datetime64(seldata))]
    #print(lno(),df)
    df1=df[['日期','成交金額','發行量加權股價指數','漲跌點數']].head(30)
    df1.columns=['date','vol','close','diff']
    df1=df1.sort_values(by=['date'], ascending=True)
    
    df1.reset_index(inplace=True)    
    ma_list = [5, 20]
    for ma in ma_list:
        df1['MA_' + str(ma)] = df1['close'].rolling(window=ma,center=False).mean()
    mv_list = [5, 10]
    for mv in mv_list:
        df1['MV_' + str(mv)] = df1['vol'].rolling(window=mv,center=False).mean()    
    if debug==2:
        print(lno(), df1['date'][df1.index[0]], df1['date'][df1.index[-1]])
        print(lno(), df1.tail(5))

    
    tStart = time.time()
    tse_l1=0
    tse_s1=0
    tse_l2=0
    tse_s2=0
    ll0=0
    ss0=0
    try:
        sel_df=comm.get_tse_exchange_data(seldata,ver=1)
        if len(df):
            tse_l1,tse_s1,tse_l2,tse_s2,ll0,ss0=get_stock_long_short_list(sel_df['stock_id'].values.tolist(),seldata) 
    except:
        pass
    tEnd = time.time()
    print ("It cost %.3f sec" % (tEnd - tStart))   
    #raise
    otc_l1=0
    otc_s1=0
    otc_l2=0
    otc_s2=0
    ll1=0
    ss1=0    
    try:
        sel_df=comm.get_otc_exchange_data(seldata,ver=1)
        if len(df):
            otc_l1,otc_s1,otc_l2,otc_s2,ll1,ss1=get_stock_long_short_list(comm.get_otc_exchange_data(seldata,ver=1)['stock_id'].values.tolist(),seldata) 
    except:
        pass
    if debug==1:
        print(lno(), df1.tail(2))            
        #print(lno(), tse_list,df_tse.dtypes)  
    date=df1.iloc[-1]['date']      
    close=df1.iloc[-1]['close']
    pclose=df1.iloc[-2]['close']
    vol=df1.iloc[-1]['vol']      
    ma5= df1.iloc[-1]['MA_5']      
    ma20= df1.iloc[-1]['MA_20'] 
    pma5= df1.iloc[-2]['MA_5']
    pma20= df1.iloc[-2]['MA_20']
    
    ma5_trend= 100*(ma5-pma5) /pma5
    ma20_trend= 100*(ma20-pma20) /pma20
    diff=ma5-ma20    
    tse_long=(tse_l1+tse_l2) /2
    tse_short=(tse_s1+tse_s2) /2
    otc_long=(otc_l1+otc_l2) /2
    otc_short=(otc_s1+otc_s2) /2
    foreign_investment=twse_big3.get_foreign_investment(date)
    op_ratio=op.get_op_ratio(date)
    taiwan_dollar_exrate=  taiwan_dollar.get_taiwan_dollor(date)
    pvol=df1.iloc[-2]['vol']      
    mv5= df1.iloc[-1]['MV_5']      
    mv10= df1.iloc[-1]['MV_10']
    special_signal=''    
    if close>=(pclose*1.012) and vol>=(pvol*1.3):
        special_signal='起漲日'
    elif close<=pclose and vol>pvol:
        special_signal='出貨日'
    elif close>=(pclose*1.006) and vol>=((mv5+mv10)/2):
        special_signal='多頭k'
    out_list=[]
    tmp1=[]
    tmp1.append(date)        
    tmp1.append(close) 
    tmp1.append(vol/100000000)     
    tmp1.append(ma5_trend)        
    tmp1.append(ma20_trend) 
    tmp1.append(diff) 
    tmp1.append(tse_long)        
    tmp1.append(tse_short)        
    tmp1.append(otc_long)        
    tmp1.append(otc_short)  
    tmp1.append(foreign_investment)        
    tmp1.append(op_ratio)        
    tmp1.append(taiwan_dollar_exrate)        
    tmp1.append(special_signal)    
    tmp1.append  (powerinx.get_tse_power_inx(date,'up'))
    tmp1.append  (powerinx.get_otc_power_inx(date,'up'))
    tmp1.append  (powerinx.get_tse_power_inx(date,'down'))
    tmp1.append  (powerinx.get_otc_power_inx(date,'down'))    
    
    opOI=op.get_opData(date)
    print(lno(),opOI,len(opOI))
    tmp1.append ("%6d %6d(%6d)"%(opOI[0][0],opOI[0][1],opOI[0][2]))
    tmp1.append ("%6d %6d(%6d)"%(opOI[1][0],opOI[1][1],opOI[1][2]))
    tmp1.append ("%6d %6d(%6d)"%(opOI[4][0],opOI[4][1],opOI[4][2]))
    tmp1.append ("%6d %6d(%6d)"%(opOI[5][0],opOI[5][1],opOI[5][2]))
    tmp1.append ("%6d %6d(%6d)"%(opOI[2][0],opOI[2][1],opOI[2][2]))
    tmp1.append ("%6d %6d(%6d)"%(opOI[6][0],opOI[6][1],opOI[6][2]))
    tmp1.append(ll0+ll1)
    tmp1.append(ss0+ss1)
    out_list.append(tmp1)
    labels = ['日期','指數', '量', '5ma趨勢','20ma趨勢', '均差', '上市多頭家數','上市空頭家數','上櫃多頭家數','上櫃空頭家數','外資買超','op OI','匯率','特殊信號','上市強勢主流','上櫃強勢主流','上市弱勢主流','上櫃弱勢主流','買權留倉最大量','買權留倉次量','賣權留倉最大量','賣權留倉次量','買權留倉增加最多','賣權留倉增加最多','多頭家數','空頭家數']
    df_fin = pd.DataFrame.from_records(out_list, columns=labels)
    print(lno(),fin_file)
    if os.path.exists(fin_file): 
        df_s = pd.read_csv(fin_file,encoding = 'utf-8')
        #df_s.dropna(axis=1,how='all',inplace=True)
        #df_s.dropna(inplace=True)
        df_s['日期']=[date_sub2time64(x) for x in df_s['日期'] ]
        df_s=df_s.append(df_fin,ignore_index=True)
        df_s.drop_duplicates(subset=['日期'],keep='last',inplace=True)
        df_s=df_s.sort_values(by=['日期'], ascending=False)
        df_s.to_csv(fin_file,encoding='utf-8', index=False)
    else :
        df_fin.to_csv(fin_file,encoding='utf-8', index=False)
    return df_fin
        
def generate_twii_fin(startdate,enddate,debug=0,regen=0):
    
    nowdatetime = enddate
    day=0
    while   nowdatetime>=startdate :
        print (lno(),nowdatetime)
        check_twii_fin(nowdatetime,regen=regen)   

        day=day+1
        nowdatetime = enddate - relativedelta(days=day)    
def generate_twii_fin_html(startdate,enddate,debug=0):
    
    nowdatetime = enddate
    day=0
    while   nowdatetime>=startdate :
        print (lno(),nowdatetime)
        show_twii_v1(nowdatetime)   

        day=day+1
        nowdatetime = enddate - relativedelta(days=day)        

if __name__ == '__main__':
    print (lno(),sys.path[0])
    #get_cur_twii_list(datetime.today())
    if len(sys.argv)==1:
        #objdatetime= datetime.today() - relativedelta(days=1)
        #fetch_twii(objdatetime,4,get_cur_twii_list(datetime.today()))
        nowdatetime=stock_comm.get_date()
        #dstpath='%s/%d%02d'%(TWIIPATH,int(nowdatetime.year), int(nowdatetime.month))
        #url_get0='http://www.twse.com.tw/exchangeReport/FMTQIK?response=csv&date=%d%02d%02d'%(int(nowdatetime.year),int(nowdatetime.month),int(nowdatetime.day))
        #get_list_form_url_and_save(url_get0,dstpath)
        download_twii(nowdatetime,nowdatetime)
        generate_twii_fin(nowdatetime,nowdatetime,regen=1)
        show_twii_v1(nowdatetime)
    elif sys.argv[1]=='-d1' :
        try:
            nowdatetime=datetime.strptime(sys.argv[2],'%Y%m%d')
        except:
            print (lno(),'func -p startdata enddate')
            raise   
        download_twii(nowdatetime,nowdatetime)
        generate_twii_fin(nowdatetime,nowdatetime,regen=1)
        show_twii_v1(nowdatetime)
    elif sys.argv[1]=='-d' :
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        download_twii(startdate,enddate)
    elif sys.argv[1]=='-g' :
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        generate_twii(startdate,enddate)    
    elif sys.argv[1]=='-s' :   
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        generate_twii_fin(startdate,enddate) 
    elif sys.argv[1]=='-h' :   
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        generate_twii_fin_html(startdate,enddate)     
    elif sys.argv[1]=='t' :   
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        check_twii_fin(startdate,regen=1)         
    else :    
        nowdatetime=datetime.strptime(sys.argv[1],'%Y%m%d')
        show_twii_v1(nowdatetime,debug=1)
        