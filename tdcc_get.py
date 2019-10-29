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
import tdcc_dist
from os import mkdir
from os.path import isdir
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)


def main():
    url='https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5'
    outf='download/TDCC_OD_1-5.csv'
    r = requests.get(url)
    print(r.status_code)
    if not r.ok:
        print("Can not get data at {}".format(url))
        return 
    open(outf, 'wb').write(r.content)
    tdcc_dist.update_tdcc_date()
    tdcc_dist.update_tdcc_data()
    
if __name__ == '__main__':
    main()
