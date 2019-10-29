# -*- coding: utf-8 -*-
#import grs
import csv
import os
import sys
import time
import logging
from datetime import datetime
from datetime import timedelta
#from grs import Stock
from stock_comm import OTCNo
from stock_comm import TWSENo
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
import pandas as pd
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
            print("I/O ReadCSVasList error({0}): {1}".format(errno, strerror))  
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
            print("I/O WriteListToCSV error({0}): {1}".format(errno, strerror))    
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
    #print r.text,len(r.text)
    datelist=  [e.strip('"').encode('utf-8') for e in r.text.strip('[]').split(',')]
    #print datelist
    SCA_DATE=[]
    for i in datelist:
        if i.isdigit() and len(i)==8 :
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
                    print (lno(),i[0])
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
    try:
        with open(fpath, encoding='utf-8-sig') as csv_file:
            csv_data = csv.reader(csv_file)
            for i in csv_data:
                if len(i[0])!=8 :
                    continue;
                
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
            post_url='https://www.tdcc.com.tw/smWeb/QryStockAjax.do'
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
        df_twse['stock'].astype(str)
        df_otc.columns = ['stock', 'name', 'd1', 'd2']
        df_otc['stock'].astype(str)
        for j in  range(0,len(df_twse)):
            for k in  range(0,len(df_date)):    
                #print lno(),df_date.iloc[k]['date']
                stock_no=df_twse.iloc[j]['stock']
                if len(stock_no)<4:
                    print (lno(),df_twse.iloc[j]['stock'])
                    continue
                ymd= df_date.iloc[k]['date']
                folderpath='data/csv/dist/%s'%stock_no
                check_dst_folder(folderpath)
                save_file=('data/csv/dist/%(stock)s/%(ymd)d_dist.csv')% {'stock': stock_no,'ymd': ymd}        
                list1= download_dist_to_csv(stock_no,ymd,save_file)
                #df=pd.read_html(url,encoding='big5hkscs',header=0)                
                print (lno(),len(list1))
        for j in  range(0,len(df_otc)):
            for k in  range(0,len(df_date)):    
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
    elif len(sys.argv)==2:   
        if sys.argv[1]=="1" :
            update_tdcc_date()
            sys.exit()
        elif sys.argv[1]=="2" :
            update_tdcc_data()
            sys.exit()
        else:   
            datatime=datetime.strptime(sys.argv[1],'%Y%m%d')
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
    """
    #test={'date':20160427,'stock_id':2618,'av_price':17.0,'high':17.9,'low':16.5,'vol':10001}
    test={}
    test['date']=20160427
    test['stock_id']=2618
    test['av_price']=17.0
    test['high']=17.9
    test['low']=16.0
    test['vol']=10001
    test['>1000_per']=17.9
    test['>1000_voladd']=2000
    test['>1000_peo']=18
    test['>200_per']=17.9
    test['>200_voladd']=4000
    test['>200_peo']=240
    test['<200_per']=17.9
    test['<200_voladd']=-2000
    test['<200_peo']=24000
    print test.keys()
    print test
    """
    """
    _dist=stock_dist(stock_no[0],datatime,data_cnt,stock.raw)
    #_dist.info()
    num=len(_dist.dist_list)
    print '%s'%(u'    日期,  均價, 最高, 最低,   張數,  千張%,   張數,人數,  兩百%,   張數,人數,  散戶%,人數    ,')
    for t in range(num):
        i=_dist.dist_list[t]
        print '%4d%02d%02d,'%(i[1].year,i[1].month,i[1].day),
        print '%3.2f,%3.2f,%3.2f,%7.0f,'%(i[2][0]/i[2][4],i[2][1],i[2][2],i[2][3]),
        if t==num-1:
            print '%3.2f%%,%7.0f,%4.0f,'%(i[3][0][2],0,i[3][0][0]),
            print '%3.2f%%,%7.0f,%4.0f,'%(i[3][1][2],0,i[3][1][0]),
            print '%3.2f%%,%7.0f,'%(i[3][2][2],i[3][2][0])
        else:
            j=_dist.dist_list[t+1]
            #print i[3][0][2]
            print '%3.2f%%,%7.0f,%4.0f,'%(i[3][0][2],i[3][0][1]/1000-j[3][0][1]/1000,i[3][0][0]),
            print '%3.2f%%,%7.0f,%4.0f,'%(i[3][1][2],i[3][1][1]/1000-j[3][1][1]/1000,i[3][1][0]),
            print '%3.2f%%,%7.0f,'%(i[3][2][2],i[3][2][0])
    """ 
        
"""
    tdcc_date=tdcc_read_date(tdcc_file,sys.argv[1])
    for h in stock_list:
        list_final=[]
        list_final.append(h)
        list_final.append(sys.argv[1])
        stock = Stock(h,datetime.strptime(sys.argv[1],'%Y%m%d'))
        for j in range(6) :  #6
            if tdcc_date[j]!=None and tdcc_date[j+1]!=None :
                #print tdcc_date[j],tdcc_date[j+1]
                stock_dist= calc_dist(get_stock_dist(h,tdcc_date[j]))
                account=0
                high=0
                low=0
                close=0
                cnt=0
                for i in stock.raw:
                    tmp_date=i[0].split ('/')
                    tmp_str="%d%0s%0s"%(int(tmp_date[0])+1911,tmp_date[1],tmp_date[2])
                    #print datetime.strptime(tmp_str,'%Y%m%d'),tdcc_date[0]
                    if datetime.strptime(tmp_str,'%Y%m%d').date() <= tdcc_date[j] and datetime.strptime(tmp_str,'%Y%m%d').date() > tdcc_date[j+1] :
                        print i
                        account+=float(i[1])
                        close+=float(i[6])
                        if float(i[4])>high:
                            high=float(i[4])
                        if low==0 or low<float(i[5]):
                            low=float(i[5])
                        cnt+=1
                list1=[]
                list1.append(close/cnt)     
                list1.append(high)      
                list1.append(low)       
                list1.append(account/cnt/1000)      
                stock_dist.append(list1)
                list_final.append(stock_dist)
        for k in list_final:
            print k

"""
