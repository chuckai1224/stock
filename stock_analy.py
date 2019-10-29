# -*- coding: utf-8 -*-
#import grs
# py -m pip install requests
import csv
import os
import sys
from datetime import datetime
from datetime import timedelta


import inspect

import requests
from inspect import currentframe, getframeinfo
"""
import inspect
import traceback
DEBUG=1
LOG=1
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def ppp(string):
    if DEBUG:
        frame = inspect.currentframe()
        stack_trace = traceback.format_stack(frame)
        logging.info(stack_trace[:-1])
    if LOG:
        logging.info(string)
"""        
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '{}-L({})'.format(filename,inspect.currentframe().f_back.f_lineno)
def getKey(item):
    return item[0]
def getKey1(item):
    return item[1]  
def getKey2(item):
    return item[2]
def getKey3(item):
    return item[3]
def getKey_final(item):
    return item[1]+item[2]+item[3]              

if __name__ == '__main__':
    print (sys.getdefaultencoding(), sys.stdout.encoding)
    #dataday=stock_comm.get_date()
    url='https://www.tdcc.com.tw/smWeb/QryStockAjax.do'
       
    dd={'REQ_OPR':'qrySelScaDates'}
    headers={'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:59.0) Gecko/20100101 Firefox/','Content-Type':'application/x-www-form-urlencoded'
}
    session = requests.Session()
    res = session.post(url=url, headers=headers, data=dd)
    #r = requests.post(post_url,header=headers, data=dd)
    #print (r.text)
    sys.exit()
