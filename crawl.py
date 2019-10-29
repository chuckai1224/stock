# -*- coding: utf-8 -*-

import os
import re
import sys
import csv
import time
import string
import logging
import requests
import argparse
from datetime import datetime, timedelta
import inspect
from inspect import currentframe, getframeinfo
from os import mkdir
from os.path import isdir
import pandas as pd
import numpy as np
import stock_comm as comm
import json
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)
def string_float(x):
            #print(lno(),type(x))
    if type(x)==str:
        if '除' in x :
            return float('0.0')
        if '--' in x:
            return np.NaN    
        return float(x.strip().replace(',',''))
    else:   
        return float(x)

def string_int(x):
    #print(lno(),type(x))
    if type(x)==str:
        return int(x.strip().replace(',',''))
    else:    
        return int(x)        
class Crawler():
    def __init__(self, prefix="{}/stock_data".format(comm.datafolder())):
        ''' Make directory if not exist when initialize '''
        if not isdir(prefix):
            mkdir(prefix)
        self.prefix = prefix
        self.stock_columns= ['date', 'vol', 'cash','open', 'high', 'low','close', 'diff', 'Tnumber','stock_name']

    def _clean_row(self, row):
        ''' Clean comma and spaces '''
        for index, content in enumerate(row):
            row[index] = re.sub(",", "", content.strip())
        return row

    def _record(self, stock_id, row):
        ''' Save row to csv file '''
        repeat=0
        if len(stock_id)>=5:
            #print("tt40",len(stock_id),stock_id)
            return
        #print(stock_id)    
        fname='{}/{}.csv'.format(self.prefix, stock_id)
        try :
            with open(fname, 'rb') as f:  #打开文件
                f.seek(0, os.SEEK_END)
                size = f.tell()
                off = -100      #设置偏移量
                while True:
                    if size+off<0:
                        break
                    #print (fname)
                    f.seek(off, 2) #seek(off, 2)表示文件指针：从文件末尾(2)开始向前50个字符(-50)
                    lines = f.readlines() #读取文件指针范围内所有行
                    #print(lno(),len(lines),lines)
                    if len(lines)>=2: #判断是否最后至少有两行，这样保证了最后一行是完整的
                        last_line = lines[-1].decode() #.split(',') #取最后一行
                        #print(lno(),row,last_line)
                        #print(lno(),row[0],last_line[0])
                        
                        if row[0] in last_line:
                            print(lno(),"repeat",row[0],stock_id)
                            repeat=1
                        break
            #如果off为50时得到的readlines只有一行内容，那么不能保证最后一行是完整的
            #所以off翻倍重新运行，直到readlines不止一行
                    off *= 2

        except :
            repeat=0    
        
        #print("tt41",len(stock_id),stock_id)    
        if repeat==1 :
            #print(lno(),"repeat",stock_id,last_line[0])
            return
        first=0    
        if os.path.exists(fname):     
            f = open(fname, 'a', encoding='utf-8')
        else:
            f = open(fname, 'w', encoding='utf-8')
            first=1
        cw = csv.writer(f, lineterminator='\n')
        #test=[]
        #test.append(u'除權 ')
        #print row
        #print test[0][0],row[3][0]
        """
        if row[7][0] == u'除' :
            #print "chuck"
            row[7] = u"0.00"
            #print row[3]
        """    
        if first==1:
            cw.writerow(self.stock_columns)
        cw.writerow(row)
        f.close()
    
    def _get_tse_data(self, date_tuple):
        date_str = '{0}{1:02d}{2:02d}'.format(date_tuple[0], date_tuple[1], date_tuple[2])
        url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX'
        query_params = {
            'date': date_str,
            'response': 'json',
            'type': 'ALLBUT0999',
            '_': str(round(time.time() * 1000) - 500)
        }

        # Get json data
        page = requests.get(url, params=query_params,timeout=30)
        if not page.ok:
            logging.error("Can not get TSE data at {}".format(date_str))
            print("Can not get TSE data at {}".format(date_str))
            return
           
        content = page.json()
        #print(content)              
        # For compatible with original data
        date_str_mingguo = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0] - 1911, date_tuple[1], date_tuple[2])
        res=[]
        now_date=datetime.strptime(date_str,'%Y%m%d')
        idx_date=datetime.strptime('20110729','%Y%m%d')
        idx_date9=datetime.strptime('20190718','%Y%m%d')
        #print(lno(),content) 
        #raise
        if now_date<=idx_date:
            text=content['data8']
        elif now_date>=idx_date9:    
            text=content['data9']
        else:
            text=content['data9']
        #print(lno(),text)    
        ## TODO fix craw tse ok need time verify
        columns = ['stock_id','stock_name', 'vol', 'Tnumber','cash','open', 'high', 'low','close','sign','diff','d1','d2','d3','d4','本益比']
        df=pd.DataFrame(text, columns=columns)
        #print(lno(),df.open)
        #df['date']=np.datetime64(now_date)
        df['date']='{0}-{1:02d}-{2:02d}'.format(date_tuple[0], date_tuple[1], date_tuple[2])
        df=df.replace('--',np.NaN)
        df=df.replace('---',np.NaN)
        df=df.replace('----',np.NaN)
        df['open']=df['open'].apply(string_float)
        df['high']=df['high'].apply(string_float)
        df['low']=df['low'].apply(string_float)
        df['close']=df['close'].apply(string_float)
        df['vol']=df['vol'].apply(string_int)
        df['cash']=df['cash'].apply(string_int)
        df['diff']=df['diff'].apply(string_float)
        def calc_diff(row):
            if row['sign'].find('green') > 0:
                return -row['diff']
            else:
                return row['diff']     
        df['diff']=df.apply(calc_diff, axis=1)
        df1=df[ ['stock_id','date', 'vol', 'cash','open', 'high', 'low','close', 'diff', 'Tnumber','stock_name']]
        #print(lno(),df1.dtypes)
        tStart = time.time()  
        def add_stock_data(row):
            if len(row['stock_id'])!=4:
                return
            #print(lno(),row['stock_id'])    
            self._record(row['stock_id'], row.tolist()[1:])
            return    
            
        df1.apply(add_stock_data,axis=1)
        out_file='{0}/exchange/tse/{1}{2:02d}{3:02d}'.format(comm.datafolder(),date_tuple[0] , date_tuple[1], date_tuple[2])
        comm.check_dst_folder(os.path.dirname(out_file))
        df1.to_csv(out_file,encoding='utf-8', index=False) 
        """
        out_file='{0}{1:02d}{2:02d}'.format(date_tuple[0] , date_tuple[1], date_tuple[2])
        df1.to_csv(out_file,encoding='utf-8', index=False) 
        dateparse = lambda dates: pd.datetime.strptime(dates,'%Y-%m-%d')
        df2 = pd.read_csv(out_file,encoding = 'utf-8',parse_dates=['date'], date_parser=dateparse)
        print(lno(),df2.dtypes)
        """
           
        tEnd = time.time()      
        print ("It cost %.3f sec" % (tEnd - tStart))   
    
    def _get_tse_data_old(self, date_tuple):
        date_str = '{0}{1:02d}{2:02d}'.format(date_tuple[0], date_tuple[1], date_tuple[2])
        url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX'
        query_params = {
            'date': date_str,
            'response': 'json',
            'type': 'ALLBUT0999',
            '_': str(round(time.time() * 1000) - 500)
        }

        # Get json data
        page = requests.get(url, params=query_params,timeout=30)

        if not page.ok:
            logging.error("Can not get TSE data at {}".format(date_str))
            print("Can not get TSE data at {}".format(date_str))
            return

        content = page.json()
        #print(content)
        # For compatible with original data
        date_str_mingguo = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0] - 1911, date_tuple[1], date_tuple[2])
        res=[]
        now_date=datetime.strptime(date_str,'%Y%m%d')
        idx_date=datetime.strptime('20110729','%Y%m%d')
        idx_date9=datetime.strptime('20190718','%Y%m%d')
        
        if now_date<=idx_date:
            text=content['data4']
        elif now_date>=idx_date9:    
            text=content['data9']
        else:
            text=content['data4']
        for data in text:
            print (len(data),data)
            sign = '-' if data[9].find('green') > 0 else ''
            row = self._clean_row([
                date_str_mingguo, # 日期
                data[2], # 成交股數
                data[4], # 成交金額
                data[5], # 開盤價
                data[6], # 最高價
                data[7], # 最低價
                data[8], # 收盤價
                sign + data[10], # 漲跌價差
                data[3], # 成交筆數
                data[1]
            ])
            res.append([data[0].strip()]+row)
    
            self._record(data[0].strip(), row)
        labels = ['stock_id','date', 'vol', 'cash','open', 'high', 'low','close', 'diff', 'Tnumber','stock_name']
        df = pd.DataFrame.from_records(res, columns=labels)
        if not os.path.isdir('csv/data/tse'):
            os.makedirs('csv/data/tse')  
        out_file='csv/data/tse/{0}{1:02d}{2:02d}'.format(date_tuple[0] , date_tuple[1], date_tuple[2])
        df.to_csv(out_file,encoding='utf-8', index=False)   
        #print("test113",df.head())
    def _get_otc_data(self, date_tuple):
        date_str = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0] - 1911, date_tuple[1], date_tuple[2])
        ttime = str(int(time.time()*100))
        url = 'http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d={}&_={}'.format(date_str, ttime)
        page = requests.get(url,timeout=30)

        if not page.ok:
            logging.error("Can not get OTC data at {}".format(date_str))
            return

        result = page.json()

        if result['reportDate'] != date_str:
            logging.error("Get error date OTC data at {}".format(date_str))
            return
        
        ## TODO fix craw otc
        res=[]
        #print(lno(),type(result['mmData']))
        #print(lno(),result['aaData'][0])
        res=result['mmData']
        res.extend(result['aaData'])
        #print(lno(),res)
        columns = ['stock_id','stock_name','close','diff', 'open', 'high', 'low', 'av','vol','cash',\
         'Tnumber','b1','s1','total_stock','d1','d2','d3']
        df=pd.DataFrame( res, columns=columns)
        #now_date=datetime.strptime('{0}{1:02d}{2:02d}'.format(date_tuple[0], date_tuple[1], date_tuple[2]),'%Y%m%d')
        #df['date']=np.datetime64(now_date)
        df['date']='{0}-{1:02d}-{2:02d}'.format(date_tuple[0], date_tuple[1], date_tuple[2])
        df=df.replace('--',np.NaN)
        df=df.replace('---',np.NaN)
        df=df.replace(' ---',np.NaN)
        df=df.replace('----',np.NaN)
        
        df['open']=df['open'].apply(string_float)
        df['high']=df['high'].apply(string_float)
        df['low']=df['low'].apply(string_float)
        df['close']=df['close'].apply(string_float)
        df['vol']=df['vol'].apply(string_int)
        df['cash']=df['cash'].apply(string_int)
        df['diff']=df['diff'].apply(string_float)
        df1=df[ ['stock_id','date', 'vol', 'cash','open', 'high', 'low','close', 'diff', 'Tnumber','stock_name']]
        #print(lno(),df1.dtypes)
        tStart = time.time()  
        def add_stock_data(row):
            #print(lno(),row)
            if len(row['stock_id'])!=4:
                return
            self._record(row['stock_id'], row.tolist()[1:])
            return    
            
        tStart = time.time()
        df1.apply(add_stock_data,axis=1)
        out_file='{0}/exchange/otc/{1}{2:02d}{3:02d}'.format(comm.datafolder(),date_tuple[0] , date_tuple[1], date_tuple[2])
        comm.check_dst_folder(os.path.dirname(out_file))
        #print(lno(),out_file)
        df1.to_csv(out_file,encoding='utf-8', index=False)
        tEnd = time.time()      
        print ("It cost %.3f sec" % (tEnd - tStart))
        #print(lno(),df)
        """
        raise
        for table in [result['mmData'], result['aaData']]:
            for tr in table:
                row = self._clean_row([
                    date_str,
                    tr[8], # 成交股數
                    tr[9], # 成交金額
                    tr[4], # 開盤價
                    tr[5], # 最高價
                    tr[6], # 最低價
                    tr[2], # 收盤價
                    tr[3], # 漲跌價差
                    tr[10], # 成交筆數
                    tr[1]
                ])
                #print("test12", tr[0])
               
                self._record(tr[0], row)
                res.append([tr[0]]+row)
        labels = ['stock_id','date', 'vol', 'cash','open', 'high', 'low','close', 'diff', 'Transactions','stock_name']
        df = pd.DataFrame.from_records(res, columns=labels)
        if not os.path.isdir('csv/data/otc'):
            os.makedirs('csv/data/otc')  
        out_file='csv/data/otc/{0}{1:02d}{2:02d}'.format(date_tuple[0] , date_tuple[1], date_tuple[2])
        df.to_csv(out_file,encoding='utf-8', index=False)
        """
    def get_data(self, date_tuple):
        print('Crawling {}'.format(date_tuple))
        self._get_tse_data(date_tuple)
        self._get_otc_data(date_tuple)

    def _get_stocks_index_data(self, date_tuple):
        date_str = '{0}{1:02d}{2:02d}'.format(date_tuple[0], date_tuple[1], date_tuple[2])
        url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX'
        
        query_params = {
            'date': date_str,
            'response': 'json',
            'type': 'IND',
            '_': str(round(time.time() * 1000) - 500)
        }

        # Get json data
        page = requests.get(url, params=query_params,timeout=30)

        if not page.ok:
            print ("page faile")
            logging.error("Can not get TSE data at {}".format(date_str))
            return

        content = page.json()
        #print (content.keys())
        #print (content)
        # For compatible with original data
        #date_str_mingguo = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0] - 1911, date_tuple[1], date_tuple[2])
        date_str_mingguo = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0] , date_tuple[1], date_tuple[2])
        idx_list=[
            u'水泥窯製類指數', u'塑膠化工類指數',u'機電類指數',
            u'水泥類指數', u'食品類指數',u'塑膠類指數',
            u'紡織纖維類指數', u'電機機械類指數',u'電器電纜類指數',
            u'化學生技醫療類指數', u'化學類指數',u'生技醫療類指數',
            u'玻璃陶瓷類指數', u'造紙類指數',u'鋼鐵類指數',
            u'橡膠類指數',  u'汽車類指數',u'電子類指數',
            u'半導體類指數', u'電腦及週邊設備類指數',u'光電類指數',
            u'通信網路類指數', u'電子零組件類指數',u'電子通路類指數',
            u'資訊服務類指數', u'其他電子類指數',u'建材營造類指數',
            u'航運類指數', u'觀光類指數',u'金融保險類指數',
            u'貿易百貨類指數', u'油電燃氣類指數',u'其他類指數',
        ]
        #print (content['data1'])
        result=[]
        for data in content['data1']:
            #print (len(content['data1']),data)
            
            if data[0] in idx_list:
                #print ("ttt",data)
                sign = '-' if data[2].find('green') > 0 else ''
                row = self._clean_row([
                data[0],
                date_str_mingguo, # 日期
                data[1], # 指數
                sign + data[3], # 漲跌價差
                data[4], # %數
                ])
                #print ("row",row)
                result.append(row)               
        path="data/stocks_index"
        if not isdir(path):
            mkdir(path)
        if (len(result)==33):    
            f = open('{}/{}.csv'.format(path, date_str_mingguo.replace('/','')), 'w')
            cw = csv.writer(f, lineterminator='\n')
            cw.writerows(result)
            f.close()           
        

    def get_stocks_index(self, date_tuple):
        print('Crawling {}'.format(date_tuple))
        self._get_stocks_index_data(date_tuple)
        

def main():
    # Set logging
    if not os.path.isdir('log'):
        os.makedirs('log')
    logging.basicConfig(filename='log/crawl-error.log',
        level=logging.ERROR,
        format='%(asctime)s\t[%(levelname)s]\t%(message)s',
        datefmt='%Y/%m/%d %H:%M:%S')

    # Get arguments
    parser = argparse.ArgumentParser(description='Crawl data at assigned day')
    parser.add_argument('day', type=int, nargs='*',
        help='assigned day (format: YYYY MM DD), default is today')
    parser.add_argument('-b', '--back', action='store_true',
        help='crawl back from assigned day until 2010/1/1')
    parser.add_argument('-c', '--check', action='store_true',
        help='crawl back 10 days for check data')
    parser.add_argument('-a', '--stocks', action='store_true',
        help='crawl back twii stocks')

    args = parser.parse_args()

    # Day only accept 0 or 3 arguments
    if len(args.day) == 0:
        date = datetime.today()
        first_day = datetime(date.year, date.month, date.day)
    elif len(args.day) == 3:
        first_day = datetime(args.day[0], args.day[1], args.day[2])
    else:
        parser.error('Date should be assigned with (YYYY MM DD) or none')
        return

    crawler = Crawler()

    # If back flag is on, crawl till 2004/2/11, else crawl one day
    if args.back or args.check or args.stocks:
        # otc first day is 2007/04/20
        # tse first day is 2004/02/11

        #last_day = datetime(2004, 2, 11) if args.back else first_day - timedelta(100)
        #last_day = datetime(2007, 4, 20) if args.back else first_day - timedelta(10)
        last_day = datetime(2010, 1, 1) if args.back else first_day - timedelta(10)
        if args.stocks:
            last_day = datetime(2010, 1, 1) 
        max_error = 5
        error_times = 0

        while error_times < max_error and first_day >= last_day:
            try:
                if args.stocks:
                    print ("test")
                    crawler.get_stocks_index((first_day.year, first_day.month, first_day.day))
                else :   
                    crawler.get_data((first_day.year, first_day.month, first_day.day))
                #error_times = 0
            except:
                date_str = first_day.strftime('%Y/%m/%d')
                error_times += 1
                if error_times > max_error :
                    logging.error('chuck Crawl raise error {}'.format(date_str))
                continue
            finally:
                first_day -= timedelta(1)
                error_times = 0
                time.sleep(3)
    else:
        crawler.get_data((first_day.year, first_day.month, first_day.day))
        comm.exchange2sql(first_day,first_day)
        #print(lno(),first_day)
        
        comm.insert_daily_stock_data(first_day)
        crawler.get_stocks_index((first_day.year, first_day.month, first_day.day))

if __name__ == '__main__':
    main()
