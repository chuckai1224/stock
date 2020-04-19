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
import pandas as pd
import tdcc_dist
from os import mkdir
from os.path import isdir
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)


def main():
    url='https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5'
    header='user-agent: Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'
    outf='download/TDCC_OD_1-5.csv'
    cmd='curl -H "{}" {} -o {}'.format(header,url,outf)
    #if os.path.exists(filename):
    #    return 
    
    os.system(cmd)
    print(lno(),"filesize:",os.path.getsize(outf))
    if os.path.getsize(outf)<2048:
        os.remove(outf)  
            
    data=pd.read_csv(outf)
    #print(lno(),data)
    if len(data):
        data.to_csv(outf,encoding='utf-8', index=False)
        savef='{}_{}'.format(outf,data.iloc[0]['資料日期'])
        print(lno(),savef)
        data.to_csv(savef,encoding='utf-8', index=False)
        tdcc_dist.update_tdcc_date()
        tdcc_dist.update_tdcc_data()
    
    """
    if not r.ok:
        print("Can not get data at {}".format(url))
        return 
    """    
    #open(outf, 'wb').write(r.content)
    #tdcc_dist.update_tdcc_date()
    #tdcc_dist.update_tdcc_data()
    
if __name__ == '__main__':
    main()
