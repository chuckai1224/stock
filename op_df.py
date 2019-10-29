import csv
import os
import sys
import time
import logging
from datetime import datetime
from datetime import timedelta
import inspect 
tdcc_file='data/tdcc_date.csv'
from inspect import currentframe, getframeinfo
from datetime import datetime
from dateutil.relativedelta import relativedelta
import inspect
import traceback
DEBUG=1
LOG=1
import logging
import requests
import numpy as np
import pandas as pd
from math import ceil
import matplotlib
from matplotlib import pyplot as plt
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def check_dst_folder(dstpath):
    if not os.path.isdir(dstpath):
        os.makedirs(dstpath) 

dst_folder='csv/op'
out_file='csv/op/optData_20190820.csv'
df = pd.read_csv(out_file,encoding = 'utf-8')
print(df)
df = df[df['交易時段'] == '一般']
df.dropna(axis=1,how='all',inplace=True)
df.dropna(inplace=True)

date = datetime.strptime('20190820','%Y%m%d')
check_dst_folder('day_reportnew/%d%02d'%(date.year,date.month))
filen='day_reportnew/%d%02d/%d%02d%02d_test.html'%(date.year,date.month,date.year,date.month,date.day)
df.to_html(filen,escape=False,index=False,sparsify=True,border=2,index_names=False)

labels =['交易日期','契約','到期月份(週別)','履約價','買賣權','未沖銷契約數','交易時段']
df = df[labels].copy()
df_call = df[df['買賣權'] == '買權'].reset_index()
df_put  = df[df['買賣權'] == '賣權'].reset_index()
print(df_call)
print(df_put)
c = len(df_call['履約價'])

df_call['diff'] = 0 
for i in range (0,c):
    df_call.at[i,'diff'] = int(df_call.at[i,'未沖銷契約數'] )+ int(df_put.at[2,'未沖銷契約數'])



