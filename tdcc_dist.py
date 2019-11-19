# -*- coding: utf-8 -*-
#import grs
import csv
import os
import sys
import time
import logging
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
#from grs import Stock
#from stock_comm import OTCNo
#from stock_comm import TWSENo
import stock_comm as comm
import inspect
#import urllib2
import lxml.html
from bs4 import BeautifulSoup  
tdcc_file='data/tdcc_date.csv'
from inspect import currentframe, getframeinfo
import inspect
import traceback
DEBUG=1
LOG=1
import logging
import requests
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from sqlalchemy.types import NVARCHAR, Float, Integer,DateTime  

from sqlalchemy import create_engine

logger = logging.getLogger()
logger.setLevel(logging.INFO)
def ppp(string):
    if DEBUG:
        frame = inspect.currentframe()
        stack_trace = traceback.format_stack(frame)
        logging.debug(stack_trace[:-1])
    if LOG:
        logging.info(string)
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)
def check_dst_folder(dstpath):
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath)       
def tdcc_read_date(csv_path,objdate):
    with open(csv_path) as csv_file:
        csv_data = csv.reader(csv_file)
        result = []
        cnt=0
        for i in csv_data:
            if datetime.strptime(i[0],'%Y%m%d') <= datetime.strptime(objdate,'%Y%m%d'):
                #print i[0]
                result.append(datetime.strptime(i[0],'%Y%m%d').date())
                cnt+=1
                if(cnt>6):
                    break
        #print result   
        return result
def stock_dist_get(stock_no,objdate):
    dist = []
    try:
        with open(csv_path) as csv_file:
            csv_data = csv.reader(csv_file)
    except AssertionError:
            return dist
    return dist
"""    
def get_html(url):
    headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
    request = urllib2.Request(url,headers=headers)
    try:
        response = urllib2.urlopen(request)
    except urllib2.HTTPError  as e:
        print (lno(),e.code)
        print (lno(),e.read())
    else:
        html = response.read()
        return html

def get_post_html(url):
    headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
    request = urllib2.Request(url,headers=headers)
    try:
        response = urllib2.urlopen(request)
    except urllib2.HTTPError  as e:
        print e.code
        print e.read()
    else:
        html = response.read()
        return html
"""
def ReadCSVasList(csv_file):
    try:
        with open(csv_file) as csvfile:
            reader = csv.reader(csvfile, dialect='excel', quoting=csv.QUOTE_NONNUMERIC)
            datalist = []
            #print lno(),type(reader)
            tmp=[]
            for row in reader:
                tmp.append(row)
            datalist = list(tmp)
            #print datalist
            return datalist
    except IOError as errno :
            print("I/O ReadCSVasList error({0}):".format(errno ))  
            print (lno(),csv_file  )
    return
def WriteListToCSV(csv_file,csv_columns,data_list):
    try:
        with open(csv_file, 'w',newline='') as csvfile:
            writer = csv.writer(csvfile, dialect='excel', quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(csv_columns)
            for data in data_list:
                writer.writerow(data)
    except IOError as errno :
            print("I/O WriteListToCSV error({0}):".format(errno ))    
    return                              
def calc_dist(sdist):
#   print sdist[0][3]
    if len(sdist)!=15 :
        return 
    dist_list=[]
    clist=[]
    clist.append(float(sdist[14][2]))
    clist.append(float(sdist[14][3]))
    clist.append(float(sdist[14][4]))
    dist_list.append(clist)
    clist=[]
    s_num=0
    s_persion=0
    s_per=0
    for i in range(10, 15):
        #print i,sdist[i][0]
        s_num += float(sdist[i][2])
        s_persion += float(sdist[i][3])
        s_per += float(sdist[i][4])
    #print  s_num,s_persion,s_per
    clist.append(s_num)
    clist.append(s_persion)
    clist.append(s_per)
    dist_list.append(clist)
    clist=[]
    s_num=0
    s_persion=0
    s_per=0
    for i in range(0, 10):
        #print i,sdist[i][0]
        s_num += float(sdist[i][2])
        s_persion += float(sdist[i][3])
        s_per += float(sdist[i][4])
    clist.append(s_num)
    clist.append(s_persion)
    clist.append(s_per)
    dist_list.append(clist) 
    return  dist_list
def get_stock_price(stock_no,objdate):
    fpath=('csv/price/%(stock)s/%(year)d%(mon)02d.csv')% {
                    'stock': stock_no,
                    'year': objdate.year ,
                    'mon': objdate.month
                    }
    url = (
            'http://www.tdcc.com.tw/smWeb/QryStock.jsp'+
            '?SCA_DATE=%(year)d%(mon)02d%(day)02d&SqlMethod=StockNo'+
            '&StockNo=%(stock)s&sub=%%ACd%%B8%%DF') % {
                    'year': objdate.year ,
                    'mon': objdate.month,
                    'day': objdate.day,
                    'stock': stock_no}
    
    dist_list=[]
    dist_list=ReadCSVasList(fpath)
#   print dist_list
    if len(dist_list)>0 :
        dist_list.pop(0)
        return dist_list
    dist_list=[]    
    html=get_html(url)
    sp = BeautifulSoup(get_html(url).decode('cp950'),"lxml")  #cp950  
    trs=sp.find_all('tr')
    for tr in trs:
        tds=tr.find_all('td')
        if len(tds)==5 :
            if tds[0].get_text().isdecimal() and int(tds[0].get_text())>0 and int(tds[0].get_text())<=15 :
                clist=[]
                clist.append(tds[0].get_text().encode('utf-8'))
                clist.append(tds[1].get_text().encode('utf-8'))
                clist.append(tds[2].get_text().replace(",", "").encode('utf-8'))
                clist.append(tds[3].get_text().replace(",", "").encode('utf-8'))
                clist.append(tds[4].get_text().replace(",", "").encode('utf-8'))
                dist_list.append(clist)
    if len(dist_list)>0:
        dstpath='data/csv/dist/%s'%stock_no
        if not os.path.isdir(dstpath):
            os.makedirs(dstpath)
        csv_columns = ['a0','a1','a2','a3','a4']    
        WriteListToCSV(fpath,csv_columns,dist_list)
    return dist_list
def update_tdcc_date():
    post_url='https://www.tdcc.com.tw/smWeb/QryStockAjax.do'
    headers={'Content-Type': 'application/x-www-form-urlencoded','Referer': 'https://www.tdcc.com.tw/smWeb/QryStock.jsp','User-Agent': 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)'}
    dd={"REQ_OPR":"qrySelScaDates"}
    tdcc_date_path='csv/tdcc_date.csv'
    session = requests.Session()
    r = session.post(url=post_url,headers=headers, data=dd)
    #print (lno(),r.text,len(r.text))
    datelist=  [e.strip('"').encode('utf-8') for e in r.text.strip('[]').split(',')]
    #print (lno(),datelist)
    SCA_DATE=[]
    for i in datelist:
        if i.isdigit() and len(i)==8 :
            #print (i,type(i))
            if i==b'20191005':
                i=b'20191004'
            SCA_DATE.append(i.decode("utf-8") )
    print (lno(),SCA_DATE)
    if len(SCA_DATE) != 0 :
        with open(tdcc_date_path) as csv_file:
            csv_data = csv.reader(csv_file)
            result=[]
            for i in csv_data:
                if i[0] in SCA_DATE:
                    result.append(i)
                    #print lno(),i
                else :
                    #print (lno(),i[0])
                    SCA_DATE.append(i[0])      
        try:
            with open(tdcc_date_path, 'w',newline='') as csvfile:
                for i in SCA_DATE:
                    #print i    
                    csvfile.write("%s,\n"%i)
        except IOError as errno:
            print("I/O error({0}): {1}".format(errno, strerror))              
def update_tdcc_data():
    fpath='download/TDCC_OD_1-5.csv'
    index=1
    cnt=0
    tmplist=[]
    ymd=''
    try:
        with open(fpath, encoding='utf-8-sig') as csv_file:
            csv_data = csv.reader(csv_file)
            
            for i in csv_data:
                #print(lno(),i[0],type(i[0]))
                if len(i[0])!=8 :
                    continue;
                if i[0]=='20191005':
                    i[0]='20191004'
                ymd=i[0]    
                if i[2]=='1' :
                    key=i[1]
                    tmplist=[]
                    index=1
                if i[1] == key and index==int(i[2])  :
                    tmp=[]    
                    tmp.append(i[2])
                    tmp.append(i[2])
                    tmp.append(i[3])
                    tmp.append(i[4])
                    tmp.append(i[5])
                    tmplist.append(tmp)
                    index+=1
                    if int(i[2])==15 :
                        dstpath='data/csv/dist/%s'%i[1]
                        fpath=('data/csv/dist/%(stock)s/%(ymd)s_dist.csv')% {'stock': i[1],'ymd':i[0] }
                        if not os.path.isdir(dstpath):
                            os.makedirs(dstpath)
                        csv_columns = ['a0','a1','a2','a3','a4'] 
                        #print lno(),  fpath 
                        WriteListToCSV(fpath,csv_columns,tmplist)   
                        
                #if cnt>30 :
                    #break
                cnt+=1    
         ## TODO need add to sql            
        if ymd !='':
            tdcc=tdcc_dist()
            tdcc.csv2sql_bydate(ymd)  
    except IndexError:
        print (lno(),'IndexError')
        pass
    except IOError:
        print (lno(),'IOError')
        pass
       
class stock_dist(object):  
    def __init__(self,stock_no=0,data_time=None,data_cnt=6,raw=None):  
        self.stock_no=stock_no  
        self.data_time=data_time
        self.data_cnt=data_cnt+1
        self.tdcc_date_path='csv/tdcc_date.csv'
        self.tdccurl='http://www.tdcc.com.tw/smWeb/QryStock.jsp'
        self.tdcc_date_list=self.get_tdcc_date_list(self.data_time,self.data_cnt)
        print (lno(),self.tdcc_date_list)
        self.dict_list=[]
        self.raw=raw
        keys = ["pdate","ndate","av_p","h","l","av_v","cnt",">1000_per",">1000_vol",">1000_add",">1000_peo",">200_per",">200_vol",">200_add",">200_peo","<200_per","<200_peo"]
        #print len(self.tdcc_date_list)
        #print self.tdcc_date_list
        i=0
        final=[]
        while i < self.data_cnt-1:
            s=dict.fromkeys(keys, None)
            tdcc_date=self.tdcc_date_list[i]
            s['pdate']=(self.tdcc_date_list[i+1])
            s['ndate']=(tdcc_date)
            tdcc_dist=self.get_stock_dist(self.stock_no,tdcc_date)
            calc_dist=self.calc_dist(tdcc_dist)
            if calc_dist !=None:
                tt=self.get_stock_dist_price_volume(self.tdcc_date_list[i+1],tdcc_date)
                #print lno(),tdcc_date,tt
                """
                if tt['cnt']!=0:
                    s['av_p']=tt[0]/tt[4]
                    s['h']=tt[1]
                    s['l']=tt[2]
                    s['av_v']=tt[3]/tt[4]
                s['cnt']=tt[4]
                """
                s.update(tt)
                if s['cnt']!=0:
                    s['av_p']=s['av_p']/s['cnt']
                    s['av_v']=s['av_v']/s['cnt']
                #print lno(),s['av_p'],s['av_v'],s['h'],s['l']
                if s['h']=='--':
                    s['h']=0.0
                if s['l']=='--':
                    s['l']=0.0  
                s['>1000_per']=calc_dist[0][2]
                s['>1000_vol']=calc_dist[0][1]/1000
                s['>1000_peo']=calc_dist[0][0]
                s['>200_per']=calc_dist[1][2]
                s['>200_vol']=calc_dist[1][1]/1000
                s['>200_peo']=calc_dist[1][0]
                s['<200_per']=calc_dist[2][2]
                s['<200_peo']=calc_dist[2][0]
                final.append(s)
            #print(tdcc_date)
            i += 1
        for j in range(len(final)-1):
            final[j]['>1000_add']=(final[j]['>1000_vol']-final[j+1]['>1000_vol'])
            final[j]['>200_add']=final[j]['>200_vol']-final[j+1]['>200_vol']
            #print j,final[j]['>1000_add'],final[j]['>200_add']
        self.dict_list.extend(final)
                
            
        
    def info(self):
        #print  self.tdcc_date_list
        print  (lno(),self.dict_list)
    def get_dict(self):
        return  self.dict_list  
    def to_time(self,str_date):
        input_date=[]
        try:
            input_date=datetime.strptime(str_date,'%Y%m%d')
            #print 'ok',str_date
        except:
            print ('to_time error',str_date)
            pass
        return  input_date
    def taiwan_to_time(self,str_date):
        input_date=[]
        tmp_date=str_date.split ('/')
        
        try:
            tmp_str="%d%0s%0s"%(int(tmp_date[0])+1911,tmp_date[1],tmp_date[2])
            input_date=datetime.strptime(tmp_str,'%Y%m%d')
            #print 'ok',str_date
        except:
            print ('taiwan_to_time error',str_date)
            return None
            pass
        return  input_date  
        
    def get_tdcc_date_list(self,datatime,data_cnt):
        with open(self.tdcc_date_path) as csv_file:
            csv_data = csv.reader(csv_file)
            result = []
            cnt=0
            for i in csv_data:
                store_datatime=self.to_time(i[0])
                if store_datatime <= datatime:
                    if cnt==0:
                        if datatime - timedelta(weeks=1) >store_datatime :
                            break
                    #logging.info("%s-%s",store_datatime,datatime)
                    result.append(store_datatime)
                    cnt+=1
                    if(cnt>=data_cnt):
                        break
            #print result 
            if  cnt ==data_cnt :
                return result
            #result1=self.update_tdcc_date()
            #result=result1[0:data_cnt]
            #print result
            return result
                
                
            
    def get_stock_dist(self,stock_no,objdate):
        folderpath='data/csv/dist/%s'%stock_no
        fpath=('data/csv/dist/%(stock)s/%(year)d%(mon)02d%(day)02d_dist.csv')% {
                        'stock': stock_no,
                        'year': objdate.year ,
                        'mon': objdate.month,
                        'day': objdate.day
                        }
        url = (
                'http://www.tdcc.com.tw/smWeb/QryStock.jsp'+
                '?scaDates=%(year)d%(mon)02d%(day)02d&scaDate=%(year)d%(mon)02d%(day)02d&SqlMethod=StockNo'+
                '&StockNo=%(stock)s&StockName=&&clkStockName=') % {
                        'year': objdate.year ,
                        'mon': objdate.month,
                        'day': objdate.day,
                        'stock': stock_no}
        SCA_DATE='%(year)d%(mon)02d%(day)02d' % {'year': objdate.year ,
                        'mon': objdate.month,
                        'day': objdate.day}
        print (lno(),SCA_DATE)                
                        
        post_url='https://www.tdcc.com.tw/smWeb/QryStockAjax.do'
        
        d={'clkStockName':'','clkStockNo':'6152','REQ_OPR':'SELECT','scaDate':'20180331','scaDates':'20180331','SqlMethod':'StockNo','StockName':'','StockNo':'6152',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0'
        }

        if not os.path.isdir(folderpath):
            os.makedirs(folderpath)     
        dist_list=[]
        #print fpath
        dist_list=ReadCSVasList(fpath)
        #print dist_list
        if dist_list!=None :
            dist_list.pop(0)
            return dist_list
        dist_list=[]    
        #html=get_html(url)
        #html=get_post_html(url,d)
        #"""
        post_url='https://www.tdcc.com.tw/smWeb/QryStockAjax.do'
        headers={'Content-Type': 'application/x-www-form-urlencoded','Referer': 'https://www.tdcc.com.tw/smWeb/QryStock.jsp','User-Agent': 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)'}
        #dd={"REQ_OPR":"qrySelScaDates"}
        #dd = {"clkStockNo": stock_no, 'REQ_OPR': 'SELECT', 'SqlMethod': 'StockNo','StockNo': stock_no, 'scaDate': SCA_DATE, 'scaDates': SCA_DATE}
        #dd = {"clkStockNo":"6152", "REQ_OPR":"SELECT", "SqlMethod":"6152","StockNo":"6152","scaDate":"20180331","'scaDates":"20180331"}
        dd = {"clkStockNo":stock_no, "REQ_OPR":"SELECT", "SqlMethod":"StockNo","StockNo":stock_no,"scaDate":SCA_DATE,"scaDates":SCA_DATE}
        session = requests.Session()
        r = session.post(url=post_url,headers=headers, data=dd)
        #print  hex(int(r.text[6])),hex(int(r.text[7])),hex(int(r.text[8]))
        #print (r.text[6],r.text[7],r.text[8])
        html=r.text
        #"""
        sp=None
        try:
            #sp = BeautifulSoup(html.decode('cp950'),"html5lib")  #cp950  
            sp = BeautifulSoup(html,"html5lib")
        except UnicodeDecodeError:
            for i in range(len(html)-1):
                #print html[i]
                if ord(html[i])==0xfd and  ord(html[i+1])==0xd8:
                    fix_html=html[:i]+html[i+2:]
                    break
            sp = BeautifulSoup(fix_html.decode('cp950'),"html5lib")  #cp950
            
        #   print error
        #   print lno(),html[6618].encode('hex'),html[6619].encode('hex')
        trs=sp.find_all('tr')
        for tr in trs:
            tds=tr.find_all('td')
            #print len(tds),tds
            if len(tds)==5 :
                if tds[0].get_text().isdecimal() and int(tds[0].get_text())>0 and int(tds[0].get_text())<=15 :
                    clist=[]
                    clist.append(tds[0].get_text().encode('utf-8'))
                    clist.append(tds[1].get_text().encode('utf-8'))
                    clist.append(tds[2].get_text().replace(",", "").encode('utf-8'))
                    clist.append(tds[3].get_text().replace(",", "").encode('utf-8'))
                    clist.append(tds[4].get_text().replace(",", "").encode('utf-8'))
                    dist_list.append(clist)
        #print  len(dist_list)      
        if len(dist_list)>0:
            dstpath='data/csv/dist/%s'%stock_no
            if not os.path.isdir(dstpath):
                os.makedirs(dstpath)
            csv_columns = ['a0','a1','a2','a3','a4']    
            WriteListToCSV(fpath,csv_columns,dist_list)
        return dist_list
    
    def calc_dist(self,sdist):
    #   print sdist[0][3]
        if len(sdist)!=15 :
            return 
        dist_list=[]
        clist=[]
        clist.append(float(sdist[14][2]))
        clist.append(float(sdist[14][3]))
        clist.append(float(sdist[14][4]))
        dist_list.append(clist)
        clist=[]
        s_num=0
        s_persion=0
        s_per=0
        for i in range(10, 15):
            #print i,sdist[i]
            s_num += float(sdist[i][2])
            s_persion += float(sdist[i][3])
            s_per += float(sdist[i][4])
        #print  s_num,s_persion,s_per
        clist.append(s_num)
        clist.append(s_persion)
        clist.append(s_per)
        dist_list.append(clist)
        clist=[]
        s_num=0
        s_persion=0
        s_per=0
        for i in range(0, 10):
            #print i,sdist[i]
            s_num += float(sdist[i][2])
            s_persion += float(sdist[i][3])
            s_per += float(sdist[i][4])
        clist.append(s_num)
        clist.append(s_persion)
        clist.append(s_per)
        dist_list.append(clist) 
        return  dist_list
    
    def get_stock_dist_price_volume(self,start_time,end_time):
        #日期,成交股數,成交金額,開盤價,最高價,最低價,收盤價,漲跌價差,成交筆數
        result=[0.0,0.0,0.0,0.0,0]
        result_ex={}
        data=self.raw
        #print len(data)
        for ii in range(len(data)):
            #print lno(),data[i]
            if data[ii][3]=='--' and ii>1:
                data[ii][3]=data[ii-1][6]
            if data[ii][4]=='--' and ii>1:
                data[ii][4]=data[ii-1][6]
            if data[ii][5]=='--' and ii>1:
                data[ii][5]=data[ii-1][6]
            if data[ii][6]=='--' and ii>1:
                #print lno(),data[i][0],data[i][6],data[i-1][6]
                data[ii][6]=data[ii-1][6]
        #print len(data)
        #for i in data:
            #print i
        result_ex['av_v']=0.0
        result_ex['av_p']=0.0
        result_ex['h']=0.0
        result_ex['l']=0.0
        result_ex['cnt']=0.0
        result_ex['bvol_price_av']=0.0
        result_ex['bvol_high']=0.0
        result_ex['bvol_low']=0.0
        result_ex['bvol']=0.0
        for i in data:
            #print i
            t=self.taiwan_to_time(i[0])
            
            if  t!=None and t>start_time and t <=end_time :
                #print lno(),t.date(),i[6],i[1]
                result[3]+=i[1]/1000 #vol
                result_ex['av_v']+=i[1]/1000
                #print t,i[1]/1000,result[3]
                try :
                    result[0]+=i[6] #average price
                    result_ex['av_p']+=i[6]
                except  TypeError:
                    print (lno(),i[0],i[6])
                    #raise 
                    
                #print i[6],result[0]
                #print lno(),i
                if i[4] > result[1] :
                    result[1]=i[4] #high
                if i[5]<result[2] or result[2]==0 :
                    result[2]=i[5]  #low
                result[4]+=1
                
                if i[4] > result_ex['h'] :
                    result_ex['h']=i[4] #high
                if i[5]<result_ex['l'] or result_ex['l']==0.0 :
                    result_ex['l']=i[5] #low
                result_ex['cnt']+=1
                if i[1]/1000 > result_ex['bvol'] :
                    result_ex['bvol_price_av']=(i[4]+i[5]+ i[6] *2)/4
                    result_ex['bvol_high']=i[4]
                    result_ex['bvol_low']=i[5]
                    result_ex['bvol']=i[1]/1000
                
        """     
        if result[4]!=0 :
            result[0]=round(result[0],3)
            result[1]=round(result[1],3)
            result[2]=round(result[2],3)
            result[3]=round(result[3],3)
        """ 
        return result_ex    
def download_dist_to_csv_new(stock_no,SCA_DATE,save_file):
    dist_list=[]
    download=1
    filename='csv/dist/tmp'
    #print fpath
    #print(lno(),save_file)
    if os.path.exists(save_file):
        return
    """
    dist_list=ReadCSVasList(save_file)
    if dist_list!=None :
        dist_list.pop(0)
        return dist_list
    dist_list=[]
    """
    count=0
    url = 'https://www.tdcc.com.tw/smWeb/QryStockAjax.do'
    query_params = {"clkStockNo":stock_no, "REQ_OPR":"SELECT", "SqlMethod":"StockNo","StockNo":stock_no,"scaDate":SCA_DATE,"scaDates":SCA_DATE}    
    if download==1:
        page = requests.post(url, data=query_params)
        if not page.ok:
            print(lno(),"Can not get data at {}".format(url))
            return 
        with open(filename, 'wb') as file:
            # A chunk of 128 bytes
            for chunk in page:
                file.write(chunk)
    dfs = pd.read_html(filename,encoding = 'big5hkscs')
    for df in dfs:
        if '序' in df.iloc[0].values:
            df.dropna(inplace=True)
            df=df.drop([0]).reset_index(drop=True)
            #print(lno(),df)    
            df.columns= ['a0','a1','a2','a3','a4']
            if len(df)==15:
                if '1,000,001以上'==df.iloc[14]['a1'] :
                    df.iloc[14]['a1']='>1,000,000'
                #print(lno(),stock_no,df,save_file)        
                df.to_csv(save_file,encoding='utf-8', index=False)
            else :
                print(lno(),stock_no,df)
            
    cnt=0
    return 
def download_dist_to_csv(stock_no,SCA_DATE,save_file):
    dist_list=[]
    #print fpath
    dist_list=ReadCSVasList(save_file)
    #print dist_list
    if dist_list!=None :
        dist_list.pop(0)
        return dist_list
    dist_list=[]
    count=0
    while (count < 5):
        try:    
            post_url='https://www.tdcc.com.tw/smWeb/QryStock.jsp'
            headers={'Content-Type': 'application/x-www-form-urlencoded','Referer': 'https://www.tdcc.com.tw/smWeb/QryStock.jsp','User-Agent': 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)'}
            dd = {"clkStockNo":stock_no, "REQ_OPR":"SELECT", "SqlMethod":"StockNo","StockNo":stock_no,"scaDate":SCA_DATE,"scaDates":SCA_DATE}
            session = requests.Session()
            r = session.post(url=post_url,headers=headers, data=dd)
            #print  hex(int(r.text[6])),hex(int(r.text[7])),hex(int(r.text[8]))
            #print (r.text[6],r.text[7],r.text[8])
            html=r.text
        except:
            print (lno(),"URL ERROR")
            time.sleep(2)
            count=count+1
            if count>5 :
                print (lno(),"fail >5")
                SystemExit
        else : 
            break   

    sp=None
    try:
        #sp = BeautifulSoup(html.decode('cp950'),"html5lib")  #cp950  
        sp = BeautifulSoup(html,"html5lib")
    except UnicodeDecodeError:
        for i in range(len(html)-1):
            #print html[i]
            if ord(html[i])==0xfd and  ord(html[i+1])==0xd8:
                fix_html=html[:i]+html[i+2:]
                break
        sp = BeautifulSoup(fix_html.decode('cp950'),"html5lib")  #cp950
        
    #   print error
    #   print lno(),html[6618].encode('hex'),html[6619].encode('hex')
    trs=sp.find_all('tr')
    for tr in trs:
        tds=tr.find_all('td')
        #print len(tds),tds
        if len(tds)==5 :
            if tds[0].get_text().isdecimal() and int(tds[0].get_text())>0 and int(tds[0].get_text())<=15 :
                clist=[]
                clist.append(tds[0].get_text().encode('utf-8'))
                clist.append(tds[1].get_text().encode('utf-8'))
                clist.append(tds[2].get_text().replace(",", "").encode('utf-8'))
                clist.append(tds[3].get_text().replace(",", "").encode('utf-8'))
                clist.append(tds[4].get_text().replace(",", "").encode('utf-8'))
                dist_list.append(clist)
    print (lno(), len(dist_list))      
    if len(dist_list)>0:
        csv_columns = ['a0','a1','a2','a3','a4']    
        WriteListToCSV(save_file,csv_columns,dist_list)
    return dist_list                        
    
def get_tdcc_dist_all_df_bydate_num(stock_no,enddate,num,debug=0):
    tdcc_date_path='csv/tdcc_date.csv'
    date_df = pd.read_csv(tdcc_date_path,encoding = 'big5',header=None)
    date_df.dropna(axis=1,how='all',inplace=True)
    date_df.columns = ['date_str']
    #print (lno(),startdate,enddate)
    
    date_df['date'] =  pd.to_datetime(date_df['date_str'], format='%Y%m%d')
    sample_df=date_df[(date_df.loc[:,"date"] <= np.datetime64(enddate))] 
    sample_df=sample_df.head(num)
    #print (lno(),sample_df)
    outcols = ['date','t_stocks','t_persons','>400_stocks','>400_percents','>400_persons','>1000_stocks','>1000_percents','>1000_persons']       
    
    o_list=[]
    for i in range(0, len(sample_df)):
        #print (lno(),sample_df.iloc[i]['date_str'])
        tdcc_dist_file=('data/csv/dist/%(stock)s/%(ymd)s_dist.csv')% {'stock': stock_no,'ymd':sample_df.iloc[i]['date_str'] }
        #print (lno(),tdcc_dist_file)
        try :
            dist_df = pd.read_csv(tdcc_dist_file,encoding = 'utf8')
        except:
            print  (lno(),'error',tdcc_dist_file)   
            continue
        dist_df.columns=['序','持股分級','人數','股數','比例(%)'] 
        dist_df['date']=sample_df.iloc[i]['date_str']
        #dist_df.drop('序', axis=1,inplace=True)
        dist_df['持股分級']=['<1','1-5','5-10','10-15','15-20','20-30','30-40','40-50','50-100','100-200','200-400','400-600','600-800','800-1000','>1000']
        #dd=dist_df.pivot(index='date',columns='持股/單位數分級', values='人數').reset_index()
        if debug==1:
            print(lno(),dist_df)    
            print(lno(),dist_df['人數'].tolist())
            print(lno(),dist_df['股數'].tolist())
        o_list.append([dist_df['持股分級'].tolist(),dist_df['人數'].tolist(),dist_df['股數'].tolist()])
    #print (lno(),o_list)
    return o_list
def get_tdcc_dist_df_bydate_num(stock_no,enddate,num,debug=0):
    tdcc_date_path='csv/tdcc_date.csv'
    date_df = pd.read_csv(tdcc_date_path,encoding = 'big5',header=None)
    date_df.dropna(axis=1,how='all',inplace=True)
    date_df.columns = ['date_str']
    #print (lno(),startdate,enddate)
    
    date_df['date'] =  pd.to_datetime(date_df['date_str'], format='%Y%m%d')
    sample_df=date_df[(date_df.loc[:,"date"] <= np.datetime64(enddate))] 
    sample_df=sample_df.head(num)
    #print (lno(),sample_df)
    columns=['<1','1-5','5-10','10-15','15-20','20-30','30-40','40-50','50-100','100-200','200-400','400-600','600-800','800-1000','>1000']
    col2=['人數','股數','比例(%)']
    outcols=['date']
    for k in col2:
        for j in columns:
            #print(lno(),j+k)
            outcols.append(j+k)
    #print(lno(),outcols)           
    df_s=pd.DataFrame(pd.np.empty(( 1, len(outcols))) * pd.np.nan, columns = outcols)
    o_list=[]
    for i in range(0, len(sample_df)):
        #print (lno(),sample_df.iloc[i]['date_str'])
        tdcc_dist_file=('data/csv/dist/%(stock)s/%(ymd)s_dist.csv')% {'stock': stock_no,'ymd':sample_df.iloc[i]['date_str'] }
        #print (lno(),tdcc_dist_file)
        try :
            dist_df = pd.read_csv(tdcc_dist_file,encoding = 'utf8')
        except:
            print  (lno(),'error',tdcc_dist_file)   
            continue
        dist_df.columns=['序','持股分級','人數','股數','比例(%)'] 
        dist_df['date']=sample_df.iloc[i]['date_str']
        #dist_df.drop('序', axis=1,inplace=True)
        dist_df['持股分級']=['<1','1-5','5-10','10-15','15-20','20-30','30-40','40-50','50-100','100-200','200-400','400-600','600-800','800-1000','>1000']
        #print(lno(),dist_df)
        
        #dd=dist_df.pivot(index='date',columns='持股/單位數分級', values='人數').reset_index()
        if debug==1:
            print(lno(),dist_df)    
            print(lno(),dist_df['人數'].tolist())
            print(lno(),dist_df['股數'].tolist())
        #o_list.append([dist_df['持股分級'].tolist(),dist_df['人數'].tolist(),dist_df['股數'].tolist()])
        tmp=[]
        tmp.append(sample_df.iloc[i]['date'])
        tmp.extend(dist_df['人數'].tolist())
        tmp.extend(dist_df['股數'].tolist())
        tmp.extend(dist_df['比例(%)'].tolist())
        o_list.append(tmp)
        #print (lno(),o_list)
        df = pd.DataFrame.from_records(o_list, columns=outcols)
        df_s=df_s.append(df,ignore_index=True)
        df_s.dropna(axis=1,how='all',inplace=True)
        df_s.dropna(inplace=True)
        df_s.drop_duplicates(subset=['date'],keep='last',inplace=True)
        df_s=df_s.sort_values(by=['date'], ascending=False)
    #print (lno(),df_s)
    return df_s  
def calc_tdcc_dist_pwr(row):
    col=['<1diff','1-5diff','5-10diff','10-15diff','15-20diff','20-30diff','30-40diff','40-50diff','50-100diff','100-200diff','200-400diff','400-600diff','600-800diff','800-1000diff','>1000diff']
    vol=0
    for i in col:
        #print(lno(),row[i])
        if row[i]>0:
            vol=vol+row[i]
    if vol==0:
        return np.nan
    return  row['tdcc_pwr']/(vol*15)       
            
def tdcc_pwr_calc(df):
    df1=df.copy()
    columns=['<1','1-5','5-10','10-15','15-20','20-30','30-40','40-50','50-100','100-200','200-400','400-600','600-800','800-1000','>1000']
    col=[]
    #print(lno(),df1.tail())
    for i in columns:
        col.append(i+'diff')
        #print(lno(),df1.iloc[-1][i+'股數'])
        #df1[i+'diff']=df1[i+'股數']-df1[i+'股數'].shift()
        df1[i+'diff']=df1.iloc[-1][i+'股數']-df1[i+'股數']
    #print(lno(),col)
    col=['<1diff','1-5diff','5-10diff','10-15diff','15-20diff','20-30diff','30-40diff','40-50diff','50-100diff','100-200diff','200-400diff','400-600diff','600-800diff','800-1000diff','>1000diff']
    df1['tdcc_pwr']=df1['<1diff']+df1['1-5diff']*2+df1['5-10diff']*3+df1['10-15diff']*4+df1['15-20diff']*5+\
                    df1['20-30diff']*6+df1['30-40diff']*7+df1['40-50diff']*8+df1['50-100diff']*9+df1['100-200diff']*10+\
                    df1['200-400diff']*11+df1['400-600diff']*12+df1['600-800diff']*13+df1['800-1000diff']*14+ df1['>1000diff']*15
    df1['tdcc_pwr']=df1.apply(calc_tdcc_dist_pwr,axis=1)
    
    #print(lno(),df1[['date','400-600diff', '600-800diff', '800-1000diff','>1000diff','tdcc_pwr']])
    return df1.round({'tdcc_pwr': 4})
def tdcc_dist_plot(stock_no,date,list,prev_list,out_f):
    fig, axes = plt.subplots(nrows=2, ncols=1)
    fig.tight_layout()
    plt.subplot(2,1,1)
    #df_call,df_put=get_need_plot_df(tmp_list[0][0],tmp_list[1][0])
    #print(lno(),df_call.index)
    index=np.arange(0,len(list[0]))
    _list_diff=[]
    persion_add=0
    for i in range(0,len(list[0])):
        _diff=list[1][i]-prev_list[1][i]
        persion_add=persion_add+_diff
        _list_diff.append(_diff)
    #index=df_call.index
    plt.barh(index, _list_diff, color ='green',label='')
    for i in range(0,len(_list_diff)) :
        if _list_diff[i]>0:
            plt.text(_list_diff[i],i,_list_diff[i],fontsize=12,ha='left',va='center')
        else:
            plt.text(_list_diff[i],i,_list_diff[i],fontsize=12,ha='right',va='center')
    plt.title("%s %d%02d%02d persion add"%(stock_no,date.year,date.month,date.day))
    
    plt.figtext(.85,.85,'%d'%(persion_add), fontsize=75, ha='center',color='green')
    plt.yticks(index,list[0])
    #plt.grid(True)
    #plt.legend()
    plt.subplot(2,1,2)

    index=np.arange(0,len(list[0]))
    _list_diff=[]
    for i in range(0,len(list[0])):
        _diff=int((list[2][i]-prev_list[2][i])/1000)
        _list_diff.append(_diff)
    #index=df_call.index
    #df['up'] = df.apply(lambda row: 1 if row['close'] >= row['open'] else 0, axis=1)
    #ax2.bar(df.query('up == 1')['dates'], df.query('up == 1')['vol'], color='r', alpha=0.7)
    #ax2.bar(df.query('up == 0')['dates'], df.query('up == 0')['vol'], color='g', alpha=0.7)
    plt.barh(index, _list_diff, color ='red',label='')
    for i in range(0,len(_list_diff)) :
        if _list_diff[i]>0:
            plt.text(_list_diff[i],i,_list_diff[i],fontsize=12,ha='left',va='center')
        else:
            plt.text(_list_diff[i],i,_list_diff[i],fontsize=12,ha='right',va='center')
    buy=0
    sell=0
    vol=0
    for i in range(0,len(_list_diff)) :
        if _list_diff[i]>0:
            buy=buy+(i+1)*_list_diff[i]
            vol=vol+_list_diff[i]
        else:    
            sell=sell+(i+1)*_list_diff[i]
        #print(lno(),i,_list_diff[i])
    #print(lno(),buy,sell,vol)    
    #print(lno(),(buy+sell)/(vol*8)*100)    
    if vol>0:
        percent=(buy+sell)/(vol*15)*100
    else:
        percent=0
    plt.title("stock add ({:.2f})".format(percent))
    plt.figtext(.2,.1,'%.2f'%(percent), fontsize=75, ha='center',color='red')
    plt.yticks(index,list[0])
    plt.grid(True)
    #plt.legend()
    #plt.show()
    fig.set_size_inches(10, 8)
    
    fig.savefig(out_f, dpi=100)
    plt.clf()
    plt.close(fig)
    return persion_add,percent
def get_tdcc_dist_bspwe(list,prev_list):

    index=np.arange(0,len(list[0]))
    _list_diff=[]
    for i in range(0,len(list[0])):
        _diff=int((list[2][i]-prev_list[2][i])/1000)
        _list_diff.append(_diff)

    buy=0
    sell=0
    vol=0
    for i in range(0,len(_list_diff)) :
        if _list_diff[i]>0:
            buy=buy+(i+1)*_list_diff[i]
            vol=vol+_list_diff[i]
        else:    
            sell=sell+(i+1)*_list_diff[i]
        #print(lno(),i,_list_diff[i])
    if vol==0:
        return -999.999
    #print(lno(),buy,sell,vol)    
    #print(lno(),(buy+sell)/(vol*8)*100)    
    percent=(buy+sell)/(vol*8)*100

    return percent    
def gen_tdcc_dist_good(enddate):
    cnt=20
    #"""
    d=comm.get_otc_exchange_data(enddate)
    d['tdcc_bspwr']=-999.9
    for j in range(0, len(d)):
        if d.at[j,'cash']<=10000000:
            continue
        stock_no=d.at[j,'stock_id']
        if len(stock_no)!=4 :
            #print(lno(),stock_no)
            continue
        #print(lno(),stock_no)
        list=get_tdcc_dist_all_df_bydate_num(stock_no,enddate,2)
        #print(lno(),len(list))
        d.at[j,'tdcc_bspwr']=get_tdcc_dist_bspwe(list[0],list[1])
    dd=d[['stock_id','stock_name','tdcc_bspwr']].copy()
    dd=dd.sort_values(by=['tdcc_bspwr'], ascending=False)
    d1=dd.head(cnt).copy()
    #d1.to_csv('csv/tdcc_good_otc.csv',encoding='utf-8', index=False)
    #"""
    d=comm.get_tse_exchange_data(enddate)
    d['tdcc_bspwr']=-999.9
    for j in range(0, len(d)):
        if d.at[j,'cash']<=10000000:
            continue
        stock_no=d.at[j,'stock_id']
        if len(stock_no)!=4 :
            #print(lno(),stock_no)
            continue
        #print(lno(),stock_no)
        list=get_tdcc_dist_all_df_bydate_num(stock_no,enddate,2)
        if len(list)!=2:
            continue
        #print(lno(),len(list))
        d.at[j,'tdcc_bspwr']=get_tdcc_dist_bspwe(list[0],list[1])
    dd=d[['stock_id','stock_name','tdcc_bspwr']].copy()
    dd=dd.sort_values(by=['tdcc_bspwr'], ascending=False)
    d2=dd.head(cnt).copy()
    d3=d1.append(d2,ignore_index=True)
    d3.to_csv('csv/tdcc_good.csv',encoding='utf-8', index=False)
    #print(lno(),dd.head(20))
def get_total_stock(enddate,stock_no):
    tdcc_date_path='csv/tdcc_date.csv'
    date_df = pd.read_csv(tdcc_date_path,encoding = 'big5',header=None)
    date_df.dropna(axis=1,how='all',inplace=True)
    date_df.columns = ['date_str']
    #print (lno(),startdate,enddate)
    
    date_df['date'] =  pd.to_datetime(date_df['date_str'], format='%Y%m%d')
    #print(lno(),date_df.dtypes)
    date1=date_df[(date_df.loc[:,"date"] <= np.datetime64(enddate))].iloc[0]['date']
    #print(lno(),date1,type(stock_no))
    tdcc_dist_file='data/csv/dist/%s/%d%02d%02d_dist.csv'% ( stock_no,date1.year,date1.month,date1.day)
        #print (lno(),tdcc_dist_file)
    try :
        dist_df = pd.read_csv(tdcc_dist_file,encoding = 'utf8')
    except:
        
        date2_df=date_df[(date_df.loc[:,"date"] > np.datetime64(enddate))].copy()
        date2_df=date2_df.sort_values(by='date', ascending=True).reset_index(drop=True)
        find=False
        for i in range(0,len(date2_df)):
            date2=date2_df.iloc[i]['date']
            tdcc_dist_file='data/csv/dist/%s/%d%02d%02d_dist.csv'% ( stock_no,date2.year,date2.month,date2.day)
            if os.path.exists(tdcc_dist_file):
                dist_df = pd.read_csv(tdcc_dist_file,encoding = 'utf8')
                find=True
                break
            dist_df=None
        if find==False:    
            print  (lno(),'error',enddate,tdcc_dist_file)
            return 0
        
    dist_df.columns=['序','持股分級','人數','股數','比例(%)'] 
    #print(lno(),dist_df['股數'].sum())
    return dist_df['股數'].sum()

class tdcc_dist():
    def __init__(self):
        
        #self.columns_index=
        # ['<1','1-5','5-10','10-15','15-20','20-30','30-40','40-50','50-100','100-200','200-400','400-600','600-800','800-1000','>1000']
        #  0     1     2      3       4       5       6      7       8        9         10        11        12        13         14
        self.columns_old=['<1.人數', '<1.股數', '<1.比例(%)', '1-5.人數', '1-5.股數', '1-5.比例(%)', '5-10.人數', '5-10.股數', '5-10.比例(%)', '10-15.人數', '10-15.股數', '10-15.比例(%)', '15-20.人數', '15-20.股數', '15-20.比例(%)', '20-30.人數', '20-30.股數', '20-30.比例(%)', '30-40.人數', '30-40.股數', '30-40.比例(%)', '40-50.人數', '40-50.股數', '40-50.比例(%)', '50-100.人數', '50-100.股數', '50-100.比例(%)', '100-200.人數', '100-200.股數', '100-200.比例(%)', '200-400.人數', '200-400.股數', '200-400.比例(%)', '400-600.人數', '400-600.股數', '400-600.比例(%)', '600-800.人數', '600-800.股數', '600-800.比例(%)', '800-1000.人數', '800-1000.股數', '800-1000.比例(%)', '>1000.人數', '>1000.股數', '>1000.比例(%)']
        self.engine = create_engine('sqlite:///sql/tdcc_dist.db', echo=False)
        self.engine_old = create_engine('sqlite:///sql/tdcc_dist_old.db', echo=False)
        self.conn=self.engine.connect()
        self.conn_old=self.engine_old.connect()
        self.columns=range(0,15*3)
        self.dtypedict = {
            'date': DateTime()
            }
    def get_df(self,stock_id):
        df=pd.read_sql(stock_id, self.engine, parse_dates=['date']) 
        return df
        pass
    def get_total_stock_num(self,stock_id,date):
        ##TODO get total stock sql
        enddate=date+relativedelta(days=7)
        startdate=date-relativedelta(months=3)
        old_date=datetime(2018,5,4)
        if date<=old_date:
            cmd='SELECT * FROM "{}" WHERE date >= "{}" and date <"{}"'.format(stock_id,startdate,enddate)
            #print(lno(),cmd)
            try:
                df=pd.read_sql(cmd, con=self.conn_old,parse_dates=['date'])  
                #print(lno(),df.iloc[0]['total_stock_num']
                return float(df.iloc[0]['total_stock_num']*1000)
            except:
                print(lno(),stock_id,'dist no data')
                return 0
        else:    
            cols=['15','16','17','18','19','20','21','22','23','24','25','26','27','28','29']
            #print(lno(),cols)
            cmd='SELECT * FROM "{}" WHERE date >= "{}" and date <"{}"'.format(stock_id,startdate,enddate)
            #print(lno(),cmd)
            try:
                df=pd.read_sql(cmd, con=self.conn,parse_dates=['date'])  
                #print(lno(),df.iloc[-1][cols].values.sum())
                return df.iloc[-1][cols].values.sum()
            except:
                print(lno(),stock_id,'dist no data')
                return 0
    def get_tdcc_1000_400(self,stock_id,date):
        ##TODO get tdcc 1000 400
        enddate=date+relativedelta(days=7)
        startdate=date-relativedelta(months=3)
        old_date=datetime(2018,5,4)
        weel_begin_date=datetime(2015,4,30) ##20150508 第2周
        if date>old_date:
            cols=['15','16','17','18','19','20','21','22','23','24','25','26','27','28','29']
            small=  ['15','16','17','18','19','20','21','22','23','24','25']
            middle= ['26','27','28']
            big=    ['29']
            #print(lno(),cols)
            cmd='SELECT * FROM "{}" WHERE date >= "{}" and date <"{}"'.format(stock_id,startdate,enddate)
            #print(lno(),cmd)
            try:
                df=pd.read_sql(cmd, con=self.conn,parse_dates=['date'])  
                #print(lno(),df.iloc[-1][cols].values.sum())
                total_diff=df.iloc[-1][cols].values.sum()-df.iloc[-2][cols].values.sum()
                b_diff=df.iloc[-1][big].values.sum()-df.iloc[-2][big].values.sum()
                m_diff=df.iloc[-1][middle].values.sum()-df.iloc[-2][middle].values.sum()
                return b_diff,m_diff
            except:
                print(lno(),stock_id,'dist no data')
                ## continue with old database
                #return 0        
        cmd='SELECT * FROM "{}" WHERE date >= "{}" and date <"{}"'.format(stock_id,startdate,enddate)
            #print(lno(),cmd)
        try:
            df=pd.read_sql(cmd, con=self.conn_old,parse_dates=['date'])  
            #print(lno(),df.iloc[0]['total_stock_num']
            ## old tdcc dist date index 從近到遠
            total_diff=df.iloc[0]['total_stock_num']-df.iloc[1]['total_stock_num']
            #print(lno(),df.iloc[0]['>1000比例'],df.iloc[1]['>1000比例']) 
            b_diff=df.iloc[0]['>1000比例']*df.iloc[0]['total_stock_num']/100 \
                -df.iloc[1]['>1000比例']*df.iloc[1]['total_stock_num']/100
            #print(lno(),df.iloc[0]['>400比例'],df.iloc[1]['>400比例'])     
            m_diff=(df.iloc[0]['>400比例']-df.iloc[0]['>1000比例'])*df.iloc[0]['total_stock_num']/100 \
                -(df.iloc[1]['>400比例']-df.iloc[1]['>1000比例'])*df.iloc[1]['total_stock_num']/100    
            #print(lno(),b_diff,m_diff)  
            #print(lno(),df.columns)  
            #print(lno(),df.iloc[0]['date']-df.iloc[1]['date'] >10)     
            return b_diff,m_diff
            
        except:
            print(lno(),stock_id,'dist no data')
            return np.nan,np.nan
            

    def get_df_bydate(self,stock_id,date):
        pass
    def tdcc_dftolist(self,df):
        df1=df[['人數','股數','比例(%)']]
        _list=[]
        for j in df1.values.tolist():
            _list.extend(j)
        return _list  
    def csv2sql_bydate(self,date_str):
        if type(date_str)==str:
            date=datetime.strptime(date_str,'%Y%m%d')
        else:
            date=date_str    
        enddate= date + relativedelta(days=1)    
        print(lno(),type(date), date)    
        #print(lno(),type(date_fix2), date_fix2)    
        ll=comm.get_tse_exchange_data(date)['stock_id'].values.tolist()+comm.get_otc_exchange_data(date)['stock_id'].values.tolist()
        """
        if type(date)==datetime:
            date='%s'%(date.strftime('%Y%m%d'))
        #print(lno(),date, type(date),len(date))      
        if len(date)!=8:
            print(lno(),'date format error {}'.format(date))
            return
        """    
        #print(lno(),date, type(date))  
        for stock_id in ll:
            if len(stock_id)!=4:
                continue
            table_names = self.engine.table_names()
            if stock_id not in table_names:
                continue
            print(lno(),stock_id)
            #ddate=datetime.strptime(date,'%Y%m%d')
            df_fin = pd.DataFrame(columns=self.columns, dtype=np.int64)
            #date_str='%d-%02d-%02d'%(ddate.year,ddate.month,ddate.day)
            cmd='SELECT * FROM "{}" WHERE date >= "{}" and date <"{}"'.format(stock_id,date,enddate)
            #print(lno(),cmd)
            df_sql=pd.read_sql(cmd, con=self.conn,parse_dates=['date'])  
            #print(lno(),len(df_sql),df_sql)
            if len(df_sql)==0:    
                filename='data/csv/dist/{}/{}_dist.csv'.format(stock_id,date.strftime('%Y%m%d'))
                dfi=pd.read_csv(filename,encoding = 'utf-8')
                dfi.columns=['序','持股分級','人數','股數','比例(%)']
                df_fin.loc[date]=dfi['人數'].values.tolist()+dfi['股數'].values.tolist()+dfi['比例(%)'].values.tolist() 
                #print(lno(),df_fin)
                df_fin.to_sql(name=stock_id, con=self.conn, if_exists='append',  index_label='date',dtype=self.dtypedict,chunksize=10)
            #raise    
        ## TODO need verify add tdcc data   
          
        print(lno(),ll)
        print(lno(),len(ll))
        pass
    def csv2sql_all(self,date):    
        ll=comm.get_tse_exchange_data(date)['stock_id'].values.tolist()+comm.get_otc_exchange_data(date)['stock_id'].values.tolist()
        check=0
        for stock_id in ll:
            #print(lno(),stock_id)
            if len(stock_id)!=4:
                continue
            table_names = self.engine.table_names()
            #if stock_id in table_names:
            #    continue
            print(lno(),stock_id)
            """
            if stock_id=='9103':
                check=1
            if check==0:
                continue
            """
            df_fin = pd.DataFrame(columns=self.columns, dtype=np.int64)
            
            FOLDER='data/csv/dist/{}'.format(stock_id)
            if not os.path.isdir(FOLDER):
                continue
            file_names = os.listdir(FOLDER)
            for file_name in file_names:
                if not file_name.endswith('.csv'):
                    continue
                #print(file_name,len(file_name))
                #df=pd.read_csv('{}/{}'.format(FOLDER, file_name),encoding = 'utf-8',dtype=dtypes)
                dfi=pd.read_csv('{}/{}'.format(FOLDER, file_name),encoding = 'utf-8')
                #print(lno(),dfi)
                if len(dfi)==0:
                    continue

                dfi.columns=['序','持股分級','人數','股數','比例(%)'] 
                #print(lno(),dfi['人數'].values.tolist()+dfi['股數'].values.tolist()+dfi['比例(%)'].values.tolist())
                
                #_list=self.tdcc_dftolist(dfi)
                #dfo=pd.DataFrame([_list],columns=self.columns)
                date_s1=file_name.replace('_dist.csv','')
                date_str='%s-%s-%s'%(date_s1[0:4],date_s1[4:6],date_s1[6:])
                date1=datetime.strptime(date_str,'%Y-%m-%d')
                #date=datetime(int(date_s1[0:4]),int(date_s1[4:6]),int(date_s1[6:]))
                print(lno(),date)
                df_fin.loc[date1]=dfi['人數'].values.tolist()+dfi['股數'].values.tolist()+dfi['比例(%)'].values.tolist()
                
            #print(lno(),len(df_fin))    
            if len(df_fin)==0:
                continue
            df_fin.sort_index( ascending=True,inplace = True)
            #print(lno(),df_fin.index)
            df_fin.to_sql(stock_id, self.engine, if_exists='replace', index_label='date',dtype=self.dtypedict,chunksize=10)
            #raise
            

def tdcc_sql(date):
    tdcc=tdcc_dist()
    tdcc.csv2sql_all(date)
    #tdcc.csv2sql_bydate(date)
def tdcc_sql_t0(date):
    tdcc=tdcc_dist()
    df=tdcc.get_df('6192')
    #print(lno(),df)
    stocks=tdcc.get_total_stock_num('6192',date)
    print(lno(),stocks)
def test_tdcc_dist_by_date(date):
    ##TODO: code >400 >1000 
    tdcc=tdcc_dist()
    df=tdcc.get_df('6192')
    #print(lno(),df)
    b_diff,m_diff=tdcc.get_tdcc_1000_400('6192',date)
    print(lno(), b_diff,m_diff)
    #return df    

def requests_get_dist( stock_no):
    path='data/dist'
    if not os.path.isdir(path):
        os.mkdir(path)
    filen='data/dist/dist_{}.html'.format(stock_no)    
    #cmd='curl.exe http://norway.twsthr.info/StockHolders.aspx?stock={0} -o data/dist/dist_{1}.html '.format(stock_no,stock_no)
    cmd='curl https://norway.twsthr.info/StockHolders.aspx?stock={0} -o data/dist/dist_{1}.html '.format(stock_no,stock_no)
    #https://norway.twsthr.info/StockHolders.aspx?stock=6131
    #if not os.path.isfile(filen):
    print (lno(),cmd)
    os.system(cmd)
def parse_dist_html(stock_no) :
    filen='data/dist/dist_{}.html'.format(stock_no)
    with open(filen, "r",encoding="utf-8") as f:
        contents = f.read()
        sp = BeautifulSoup(contents,"lxml")  #cp950  
        tables=sp.find_all('table')
        for table in tables:
            #print (lno(),len(table))
            tds=table.find_all('td')
            if len(tds)>=3 and '資料日期' in tds[2]:
                #print (lno(),len(tds),tds[2:35])
                res_list=[]
                tmp=[]
                for i in range(18,len(tds) ):
                #for i in range(18,52):
                    #print (lno(),i,tds[i].get_text())
                    text_str=tds[i].get_text().replace(chr(0xa0), '').replace(',', '')
                    if (i%16==2):
                        tmp=[]
                        if text_str=='2015-03':
                            tmp.append('20150331')
                        elif text_str=='2015-02':
                            tmp.append('20150226')
                        elif text_str=='2015-01':
                            tmp.append('20150130')   

                        elif text_str=='2014-12':
                            tmp.append('20141231')
                        elif text_str=='2014-11':
                            tmp.append('20141128')
                        elif text_str=='2014-10':
                            tmp.append('20141031')
                        elif text_str=='2014-09':
                            tmp.append('20140930')    
                        elif text_str=='2014-08':
                            tmp.append('20140829')    
                        elif text_str=='2014-07':
                            tmp.append('20140731')
                        elif text_str=='2014-06':
                            tmp.append('20140630')
                        elif text_str=='2014-05':
                            tmp.append('20140530')
                        elif text_str=='2014-04':
                            tmp.append('20140430')    
                        elif text_str=='2014-03':
                            tmp.append('20140331')  
                        elif text_str=='2014-02':
                            tmp.append('20140227')    
                        elif text_str=='2014-01':
                            tmp.append('20140127')      

                        elif text_str=='2013-12':
                            tmp.append('20131231')
                        elif text_str=='2013-11':
                            tmp.append('20131129')
                        elif text_str=='2013-10':
                            tmp.append('20131031')
                        elif text_str=='2013-09':
                            tmp.append('20130930')    
                        elif text_str=='2013-08':
                            tmp.append('20130830')    
                        elif text_str=='2013-07':
                            tmp.append('20130731')
                        elif text_str=='2013-06':
                            tmp.append('20130628')
                        elif text_str=='2013-05':
                            tmp.append('20130531')
                        elif text_str=='2013-04':
                            tmp.append('20130430')    
                        elif text_str=='2013-03':
                            tmp.append('20130329')  
                        elif text_str=='2013-02':
                            tmp.append('20130227')    
                        elif text_str=='2013-01':
                            tmp.append('20130131') 

                        elif text_str=='2012-12':
                            tmp.append('20121228')
                        elif text_str=='2012-11':
                            tmp.append('20121130')
                        elif text_str=='2012-10':
                            tmp.append('20121031')
                        elif text_str=='2012-09':
                            tmp.append('20120928')    
                        elif text_str=='2012-08':
                            tmp.append('20120831')    
                        elif text_str=='2012-07':
                            tmp.append('20120731')
                        elif text_str=='2012-06':
                            tmp.append('20120629')
                        elif text_str=='2012-05':
                            tmp.append('20120531')
                        elif text_str=='2012-04':
                            tmp.append('20120430')    
                        elif text_str=='2012-03':
                            tmp.append('20120330')  
                        elif text_str=='2012-02':
                            tmp.append('20120229')    
                        elif text_str=='2012-01':
                            tmp.append('20120131')  

                        elif text_str=='2011-12':
                            tmp.append('20111230')
                        elif text_str=='2011-11':
                            tmp.append('20111130')
                        elif text_str=='2011-10':
                            tmp.append('20111031')
                        elif text_str=='2011-09':
                            tmp.append('20110930')    
                        elif text_str=='2011-08':
                            tmp.append('20110831')    
                        elif text_str=='2011-07':
                            tmp.append('20110729')
                        elif text_str=='2011-06':
                            tmp.append('20110630')
                        elif text_str=='2011-05':
                            tmp.append('20110531')
                        elif text_str=='2011-04':
                            tmp.append('20110429')    
                        elif text_str=='2011-03':
                            tmp.append('20110331')  
                        elif text_str=='2011-02':
                            tmp.append('20110225')    
                        elif text_str=='2011-01':
                            tmp.append('20110128')
                        elif text_str=='2010-12':
                            tmp.append('20101231')  
                        else :   
                            tmp.append(text_str)
                    else :
                        tmp.append(text_str)    
                    if (i%16==15):
                        #print (lno(),tmp)
                        #if (len(tmp)==12):
                        #    print (lno(),tmp)
                        res_list.append(tmp)


                filen='data/dist/dist_{}.csv'.format(stock_no)
                with open(filen, 'w',encoding='utf8',newline='') as csv_file:
                    output = csv.writer(csv_file)
                    for i in res_list:
                        output.writerow(i)   
def stock_old_dist_tosql(stock_id):
    #df_fin = pd.DataFrame(columns=self.columns, dtype=np.int64)
      
    FOLDER='data/dist'
    if not os.path.isdir(FOLDER):
        return
    engine = create_engine('sqlite:///sql/tdcc_dist_old.db', echo=False)
    conn=engine.connect()    
    file_name = '%s/dist_%s.csv'%(FOLDER,stock_id)
    dfi=pd.read_csv(file_name,encoding = 'utf-8',header=None,usecols=[0,1,2,3,4,5,6,7,8,9,10,11],dtype={0:str}) 
    columns=['date','total_stock_num','總股東人數','平均張數','>400股數','>400比例','>400人數','400-600人數','600-800人數','800-1000人數','>1000人數','>1000比例']
    dfi.columns=columns  
    def str2date(x):
        return datetime.strptime(x,'%Y%m%d')
    dfi['date']=dfi['date'].apply(str2date) 
    dfi=dfi.sort_values(by=['date'], ascending=False)
    #print(lno(),dfi) 
    #print(lno(),stock_id) 
    dfi.to_sql(stock_id, engine, if_exists='replace', index=False,chunksize=10)                             
def old_dist_tosql():
    #df_fin = pd.DataFrame(columns=self.columns, dtype=np.int64)
      
    FOLDER='data/dist'
    if not os.path.isdir(FOLDER):
        return
    engine = create_engine('sqlite:///sql/tdcc_dist_old.db', echo=False)
    conn=engine.connect()    
    file_names = os.listdir(FOLDER)
    for file_name in file_names:
        if not file_name.endswith('.csv'):
            continue
        if not 'dist_' in file_name:
            continue
        print(file_name,len(file_name))
        
        stock_id=file_name.replace('.csv','').replace('dist_','')
        #df=pd.read_csv('{}/{}'.format(FOLDER, file_name),encoding = 'utf-8',dtype=dtypes)
        dfi=pd.read_csv('{}/{}'.format(FOLDER, file_name),encoding = 'utf-8',header=None,usecols=[0,1,2,3,4,5,6,7,8,9,10,11],dtype={0:str}) 
        columns=['date','total_stock_num','總股東人數','平均張數','>400股數','>400比例','>400人數','400-600人數','600-800人數','800-1000人數','>1000人數','>1000比例']
        dfi.columns=columns  
        def str2date(x):
            return datetime.strptime(x,'%Y%m%d')
        dfi['date']=dfi['date'].apply(str2date) 

        dfi=dfi.sort_values(by=['date'], ascending=False)
        #print(lno(),dfi) 
        #print(lno(),stock_id) 
        dfi.to_sql(stock_id, engine, if_exists='replace', index=False,chunksize=10)
        #raise

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv)==1:
        today= datetime.today().date()
        datatime=datetime(year=today.year,month=today.month,day=today.day,)
        logging.info( datatime)
    elif sys.argv[1]=='-d' :
        df_date = pd.read_csv('csv/tdcc_date.csv',header=None)
        df_date.dropna(axis=1,how='all',inplace=True)
        df_date.columns = ['date']
        df_date.sort_values('date', ascending=True,inplace=True)
        #print lno(),df_date
        #sys.exit()   
        df_otc = pd.read_csv('csv/otc_list.csv',encoding = 'utf-8-sig',header=None)
        df_twse = pd.read_csv('csv/twse_list.csv',encoding = 'utf-8-sig',header=None)

        df_twse.columns = ['stock', 'name', 'd1', 'd2']
        df_twse['stock']=df_twse['stock'].astype(str)
        df_otc.columns = ['stock', 'name', 'd1', 'd2']
        df_otc['stock']=df_otc['stock'].astype(str)
        i=0
        for j in  range(0,len(df_twse)):
            #for k in  range(0,len(df_date)):    
            for k in  range(len(df_date)-1,len(df_date)-2,-1):    
                print( lno(),df_date.iloc[k]['date'])
                stock_no=df_twse.iloc[j]['stock']
                if len(stock_no)<4:
                    #print (lno(),df_twse.iloc[j]['stock'])
                    continue
                ymd= df_date.iloc[k]['date']
                folderpath='data/csv/dist/%s'%stock_no
                check_dst_folder(folderpath)
                save_file=('data/csv/dist/%(stock)s/%(ymd)d_dist.csv')% {'stock': stock_no,'ymd': ymd}        
                list1= download_dist_to_csv(stock_no,ymd,save_file)
                i=i+1
                #df=pd.read_html(url,encoding='big5hkscs',header=0)                
                print (lno(),len(list1))
                
            if i>1:
                sys.exit() 
        for j in  range(0,len(df_otc)):
            #for k in  range(0,len(df_date)):    
            for k in  range(0,1):
                #print lno(),df_date.iloc[k]['date']
                stock_no=df_twse.iloc[j]['stock']
                ymd= df_date.iloc[k]['date']
                folderpath='data/csv/dist/%s'%stock_no
                check_dst_folder(folderpath)
                save_file=('data/csv/dist/%(stock)s/%(ymd)d_dist.csv')% {'stock': stock_no,'ymd': ymd}        
                list1= download_dist_to_csv(stock_no,ymd,save_file)
                #df=pd.read_html(url,encoding='big5hkscs',header=0)                
                print (lno(),len(list1))        
        #print lno(),url
        #down_tse_kline(startdate,enddate)
        sys.exit()     
    elif sys.argv[1]=='-d1' :    
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        d1=comm.get_tse_exchange_data(startdate)
        d2=comm.get_otc_exchange_data(startdate)
        #print(lno(),d1['stock_id'].values.tolist())
        d=d1['stock_id'].values.tolist()+d2['stock_id'].values.tolist()
        #print(lno(),d)
        for j in d:
            stock_no=j
            if len(stock_no)==6:
                continue
            #print(lno(),stock_no)
            ymd='%d%02d%02d'%( startdate.year,startdate.month,startdate.day)
            folderpath='data/csv/dist/%s'%stock_no
            check_dst_folder(folderpath)
            save_file=('data/csv/dist/%(stock)s/%(ymd)s_dist.csv')% {'stock': stock_no,'ymd': ymd}        
            download_dist_to_csv_new(stock_no,ymd,save_file)
            
    elif len(sys.argv)==2:   
        if sys.argv[1]=="1" :
            update_tdcc_date()
            sys.exit()
        elif sys.argv[1]=="t" :
            update_tdcc_data()
            sys.exit()
        else:   
            datatime=datetime.strptime(sys.argv[1],'%Y%m%d')
    elif sys.argv[1]=="-t" :
        stock_no=sys.argv[2]
        dataday=datetime.strptime(sys.argv[3],'%Y%m%d')
        list=get_tdcc_dist_all_df_bydate_num(stock_no,dataday,2)
        #print(lno(),list[0],list[1])
        filen='out/%s_%d%02d%02d.png'%(stock_no,dataday.year,dataday.month,dataday.day)
        tdcc_dist_plot(stock_no,dataday,list[0],list[1],filen)
    elif sys.argv[1]=="get" :
        dataday=datetime.strptime(sys.argv[2],'%Y%m%d')
        test_tdcc_dist_by_date(dataday)    
    elif sys.argv[1]=="good" :
        dataday=datetime.strptime(sys.argv[2],'%Y%m%d')
        gen_tdcc_dist_good(dataday)
    elif sys.argv[1]=="sql" :
        dataday=datetime.strptime(sys.argv[2],'%Y%m%d')
        tdcc_sql(dataday)
    elif sys.argv[1]=="sql_t0" :
        dataday=datetime.strptime(sys.argv[2],'%Y%m%d')
        tdcc_sql_t0(dataday)
    elif sys.argv[1]=="old" :
        stock_id=sys.argv[2]
        requests_get_dist(stock_id)
        parse_dist_html(stock_id)
        stock_old_dist_tosql(stock_id)
        #old_dist_tosql()    
    else:
        print (lno(),"unsport ")
        sys.exit()
        stock_no=['2348']
        data_cnt=7
        stock = Stock(stock_no[0],datatime)
        #print stock.raw
        _dist=stock_dist(stock_no[0],datatime,data_cnt,stock.raw)
        info=_dist.get_dict()
        print (len(info))
        for i in info :
            print (i)
    #print info[1]
    #_dist.info()
  
