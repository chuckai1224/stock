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
import stock_comm
from datetime import datetime, timedelta
from os import mkdir
from os.path import isdir
import pandas as pd
class Crawler():
    def __init__(self, prefix="data"):
        ''' Make directory if not exist when initialize '''
        if not isdir(prefix):
            mkdir(prefix)
        self.prefix = prefix

    def _clean_row(self, row):
        ''' Clean comma and spaces '''
        for index, content in enumerate(row):
            row[index] = re.sub(",", "", content.strip())
        return row

    def _record(self, stock_id, row):
        ''' Save row to csv file '''
        if self.cnt==0 :
            f = open('{}'.format(self.filename), 'wb')
            f.write(u'\ufeff'.encode('utf8'))
            f.close
        #else:
        f = open('{}'.format(self.filename), 'a')      
      
        cw = csv.writer(f, lineterminator='\n')
        
        cw.writerow(row.encode('utf8'))
        f.close()
        self.cnt+=1

    def _get_tse_data(self, date_tuple):
        self.filename="csv/twse_list.csv"
        self.cnt=0
        date_str = '{0}{1:02d}{2:02d}'.format(date_tuple[0], date_tuple[1], date_tuple[2])
        url = 'https://www.twse.com.tw/exchangeReport/MI_INDEX'
        query_params = {
            'date': date_str,
            'response': 'json',
            'type': 'ALLBUT0999',
            '_': str(round(time.time() * 1000) - 500)
        }

        # Get json data
        page = requests.get(url, params=query_params)

        if not page.ok:
            logging.error("Can not get TSE data at {}".format(date_str))
            return
        if os.path.isfile(self.filename): 
            os.remove(self.filename)
        content = page.json()

        # For compatible with original data
        date_str_mingguo = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0] - 1911, date_tuple[1], date_tuple[2])
        #print content
        tmp=[]
        for data in content['data9']:
            #print  data[0],len(data[0])
            if len(data[0])!=4 :
                continue
            #print  data[0],len(data[0])
            row = self._clean_row([
                data[0], # 股票代號
                data[1], # 公司名稱
                "dummy", #
                "上市股票", #
            ])
            tmp.append([data[0],data[1],'dummy','上市股票'])
            #self._record(data[0].strip(), row)
        labels = ['no','name','d1','d2']
        df_save = pd.DataFrame.from_records(tmp, columns=labels)
        df_save.to_csv(self.filename,encoding='utf-8', index=False,header=0) 
    def _get_otc_data(self, date_tuple):
        self.filename="csv/otc_list.csv"
        self.cnt=0
        date_str = '{0}/{1:02d}/{2:02d}'.format(date_tuple[0] - 1911, date_tuple[1], date_tuple[2])
        ttime = str(int(time.time()*100))
        url = 'http://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d={}&_={}'.format(date_str, ttime)
        page = requests.get(url)

        if not page.ok:
            logging.error("Can not get OTC data at {}".format(date_str))
            return

        result = page.json()

        if result['reportDate'] != date_str:
            logging.error("Get error date OTC data at {}".format(date_str))
            return
        tmp=[]
        for table in [result['mmData'], result['aaData']]:
            for tr in table:
                if len(tr[0])!=4 :
                    continue
                row = self._clean_row([
                    tr[0],
                    tr[1],
                    "dummy",
                    "上櫃股票",
                ])
                #print  tr[0],tr[1]
                tmp.append([tr[0],tr[1],'dummy','上櫃股票'])
                #self._record(tr[0], row)
        labels = ['no','name','d1','d2']
        df_save = pd.DataFrame.from_records(tmp, columns=labels)
        df_save.to_csv(self.filename,encoding='utf-8', index=False,header=0)     


    def get_data(self, date_tuple):
        print('Crawling {}'.format(date_tuple))
        self._get_tse_data(date_tuple)
        self._get_otc_data(date_tuple)

def main():
    # Set logging
    if not os.path.isdir('log'):
        os.makedirs('log')
    logging.basicConfig(filename='log/crawl-error.log',
        level=logging.ERROR,
        format='%(asctime)s\t[%(levelname)s]\t%(message)s',
        datefmt='%Y/%m/%d %H:%M:%S')
    first_day=stock_comm.get_date()  
    #first_day = datetime(2018,8,23) 
    crawler = Crawler()
    crawler.get_data((first_day.year, first_day.month, first_day.day))    

if __name__ == '__main__':
    main()

