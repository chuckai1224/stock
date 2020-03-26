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
    #https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5 --insecure
    outf='download/TDCC_OD_1-5.csv'
    #r = requests.get(url)
    #print(r.status_code)
    data=pd.read_csv(url)
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
