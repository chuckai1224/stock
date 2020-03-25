# 
# _*_ coding:UTF-8 _*_ 
#import pandas
import pandas as pd
import csv
import os
import sys
import time
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import inspect
from inspect import currentframe, getframeinfo
import kline
import twse_big3
import taiwan_dollar
import op
import powerinx
import crawl
import stock_bs_analy
import big3_fut_op
import twii
import stock_big3
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)


if __name__ == '__main__':
    print (lno(),'usage: func -d startdate enddate  or func -d')
    if sys.argv[1]=='-d' :
        try:
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        except:
            try:    
                startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            except:
                today= datetime.today().date()
                startdate=datetime(year=today.year,month=today.month,day=today.day,)
            enddate=startdate
        print(lno(),startdate,enddate) 
        """   
        kline.down_tse_kline(startdate,enddate)
        twse_big3.down_twse_3big(startdate,enddate)
        twse_big3.generate_twse_3big(startdate,enddate)
        twse_big3.download_big8()
        taiwan_dollar.down_taiwan_dollar(startdate,enddate)  
        op.op_down_load_job(startdate,enddate)  
        powerinx.download_job(startdate,enddate)  
        if startdate==enddate:
            crawl.download_job(startdate,enddate)  
            stock_bs_analy.gen_stock_bs_oneday(startdate)
        """    
        big3_fut_op.down_fut_op_big3_bydate(startdate,enddate)  
        
        twii.download_twii(startdate,enddate) 
        twii.generate_twii_fin(startdate,enddate) 
        if startdate==enddate:
            twii.show_twii_v1(startdate,debug=1)  
        """
        sb3=stock_big3.stock_big3()
        sb3.download_by_dates(startdate,enddate)   
        """
"""
資金流向
新興(土耳其 阿根廷 南非)>> 亞洲>>歐美

    下载字体

下载中文字体 SimHei.ttf

    删除当前用户 matplotlib 的缓冲文件

1
2

	

$cd ~/.cache/matplotlib
$rm -rf *.*

    添加字体

    首先在终端中进入你的环境
    查看 matplotlib 配置文件位置

    将下载的字体放到 fonts/ttf 文件夹

    编辑配置文件 matplotlibrc

#font.sans-serif:DejaVu Sans, Bitstream Vera Sans, Lucida Grande, Verdana, Geneva, Lucid, Arial, Helvetica, Avant Garde, sans-serif

修改为

font.sans-serif: SimHei, DejaVu Sans, Bitstream Vera Sans, Lucida Grande, Verdana, Geneva, Lucid, Arial, Helvetica, Avant Garde, sans-serif

修改的地方就是去掉了 # ，添加了下载的字体 SimHei

    重启 Anaconda
"""        

        
        
            
        