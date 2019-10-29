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
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)

def Time64ToTwdate(x):
    ts = pd.to_datetime(str(x)) 
    d = ts.strftime('%Y/%m/%d')
    #print (d)
    tmp_list=d.split ('/')
    print (tmp_list)
    try :
        tmp_str="%d/%0s/%0s"%(int(tmp_list[0])-1911,tmp_list[1],tmp_list[2])
        
    except :
    #print tmp_list[0]
        print (lno(),tmp_list[0])
        pass
    return tmp_str
def Time64ToDate(x):
    ts = pd.to_datetime(str(x)) 
    d = ts.strftime('%Y%m%d')
    return d
    
def download_otc_sectinx(dataday):
    url='http://www.tpex.org.tw/web/stock/aftertrading/all_daily_index/sectinx_print.php?l=zh-tw&d={}&s=undefined'.format(Time64ToTwdate(dataday))
    try:
        dfs=pd.read_html(url)
    except:
        print(lno(),url)
        return 
    #dfs=pandas.read_html('http://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date=20180925&type=MS')
    #顯示未排版過的資料
    #print (dfs[0])
    #陣列屬性:<class 'pandas.core.frame.DataFrame'>
    #print (type(dfs[0]))
    #陣列個數
    #print (len(dfs))
    if len(dfs)==0:
        return
    #將資料集指向gold參數
    gold=dfs[0]
    gold=gold.dropna(how='any',axis=0)
    #print(list(gold))
    #列出前5欄資訊
    #gold.ix[:,0:5]
    #欄位名稱自訂義
    gold.columns=['name','close','diff','open','high','low']
    filen='data/inx/{}_otc.csv'.format(Time64ToDate(dataday))
    gold.to_csv(filen,encoding='utf-8', index=False)
    #印出結果
    
    print (gold)
idx_list=[
u'水泥類指數', u'食品類指數',u'塑膠類指數',u'紡織纖維類指數', u'電機機械類指數',
u'電器電纜類指數',u'化學生技醫療類指數',u'玻璃陶瓷類指數', u'造紙類指數',u'鋼鐵類指數',
u'橡膠類指數',u'汽車類指數',u'電子類指數',u'建材營造類指數',u'航運類指數',
u'觀光類指數',u'金融保險類指數',u'貿易百貨類指數',u'綜合類指數',u'其他類指數',
u'化學類指數',u'生技醫療類指數',u'油電燃氣類指數',u'半導體類指數', u'電腦及週邊設備類指數',
u'光電類指數',u'通信網路類指數',u'電子零組件類指數',u'電子通路類指數',u'資訊服務類指數', 
u'其他電子類指數',
        ]
convert_twse_idx_list={
'水泥類指數':'水泥工業', '食品類指數':'食品工業','塑膠類指數':'塑膠工業','紡織纖維類指數':'紡織纖維', 
'電機機械類指數':'電機機械','電器電纜類指數':'電器電纜','化學生技醫療類指數':'化學生技醫療',
'玻璃陶瓷類指數':'玻璃陶瓷', '造紙類指數':'造紙工業','鋼鐵類指數':'鋼鐵工業','橡膠類指數':'橡膠工業',
'汽車類指數':'汽車工業','電子類指數':'電子工業','建材營造類指數':'建材營造','航運類指數':'航運業',
'觀光類指數':'觀光事業','金融保險類指數':'金融保險','貿易百貨類指數':'貿易百貨','綜合類指數':'綜合',
'其他類指數':'其他','化學類指數':'化學工業','生技醫療類指數':'生技醫療業','油電燃氣類指數':'油電燃氣業',
'半導體類指數':'半導體業', '電腦及週邊設備類指數':'電腦及週邊設備業','光電類指數':'光電業','通信網路類指數':'通信網路業',
'電子零組件類指數':'電子零組件業','電子通路類指數':'電子通路業','資訊服務類指數':'資訊服務業', '其他電子類指數':'其他電子業'};

otc_dict={
'食品工業':'02','塑膠工業':'03','紡織纖維':'04','電機機械':'05','電器電纜':'06','化學工業':'21','玻璃陶瓷':'08',
'鋼鐵工業':'10','橡膠工業':'11','建材營造':'14','航運業':'15','觀光事業':'16','金融業':'17','貿易百貨':'18',
'其他':'20','生技醫療類':'22','油電燃氣類':'23','半導體類':'24','電腦及週邊類':'25','光電業類':'26','通信網路類':'27',
'電子零組件類':'28','電子通路類':'29','資訊服務類':'30','其他電子類':'31','文化創意業':'32','農業科技業':'33','電子商務業':'34',
'管理股票':'80'
};

convert_otc_idx_list={
'紡織纖維':'紡織纖維', 
'電機機械':'電機機械', 
'鋼鐵工業':'鋼鐵工業',
'電子工業':'電子工業',
'建材營造':'建材營造',
'航運業':'航運業',
'觀光事業':'觀光事業', 
'其他':'其他', 
'化學工業':'化學工業',
'生技醫療':'生技醫療類',
'半導體業':'半導體類', 
'電腦及週邊設備業':'電腦及週邊類',
'光電業':'光電業類',
'通信網路業':'通信網路業',
'電子零組件業':'電子零組件類',
'電子通路業':'電子通路類',
'資訊服務業':'資訊服務類',
'其他電子業':'其他電子類',
'文化創意業':'文化創意業',
'線上遊戲業':'線上遊戲業',
'櫃買指數':''
};        
def download_tse_sectinx(dataday):
    url='http://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date={}&type=MS'.format(Time64ToDate(dataday))
    try:
        time.sleep(3)
        dfs=pd.read_html(url)
    except:
        print(lno(),url)
        return 
    #dfs=pandas.read_html('http://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date=20180925&type=MS')
    #顯示未排版過的資料
    #print (dfs[0])
    #陣列屬性:<class 'pandas.core.frame.DataFrame'>
    #print (type(dfs[0]))
    #陣列個數
    print (len(dfs))
    if len(dfs)==0:
        return
    #將資料集指向gold參數
    gold=dfs[0]
    gold.columns=['name','close','sign','diff','diff_percent']
    print (list(gold))
    tmp=[]
    for i in range (0,len(gold)):
        if gold.ix[i]['name'] in idx_list:
            print (gold.iloc[i].values.tolist())
            tmp.append(gold.iloc[i].values.tolist())
    labels =['name','close','sign','diff','diff_percent']
    df = pd.DataFrame.from_records(tmp, columns=labels)        
    #print (df)  
    filen='data/inx/{}_tse.csv'.format(Time64ToDate(dataday))
    df.to_csv(filen,encoding='utf-8', index=False)    
def try_float(x):
    try:
        #print (lno(),x)
        xx=float(x)
    except:
        xx=0
    return xx  
def get_tse_power_inx(dataday,mode):
    filen='data/inx/{}_tse.csv'.format(Time64ToDate(dataday))
    if not os.path.exists(filen):
        download_tse_sectinx(dataday)
    if os.path.exists(filen):   
        df = pd.read_csv(filen,encoding = 'utf-8')
        df.dropna(axis=1,how='all',inplace=True)
        df.dropna(inplace=True)
        df.drop('close', axis=1, inplace = True)
        df.drop('sign', axis=1, inplace = True)
        df.drop('diff', axis=1, inplace = True)
        df['diff_percent']=[try_float(x) for x in df['diff_percent'] ] 
        #print (lno(),df)
        if mode=='up':
            df1=df[(df.loc[:,"diff_percent"] >= 1) ].sort_values(by='diff_percent', ascending=False)
        else :
            df1=df[(df.loc[:,"diff_percent"] <= -1) ].sort_values(by='diff_percent', ascending=True)
        df1.reset_index(inplace=True)
        #print (lno(),df1)
        tmp=[]
        for i in range(0,len(df1)):
            #print (lno(),df1.at[i,'name'])
            if convert_twse_idx_list[df1.at[i,'name']]!='':
                str='{}({})'.format(convert_twse_idx_list[df1.at[i,'name']],df1.at[i,'diff_percent'])
                tmp.append(str)
        #print(lno(),list(df1),df1['name'].values.tolist())
        return tmp
    else :
        return []
        
def get_otc_power_inx(dataday,mode):
    filen='data/inx/{}_otc.csv'.format(Time64ToDate(dataday))
    if not os.path.exists(filen):
        download_otc_sectinx(dataday)
    if os.path.exists(filen):    
        df = pd.read_csv(filen,encoding = 'utf-8')
        df.dropna(axis=1,how='all',inplace=True)
        df.dropna(inplace=True)
        df['diff_percent']=df['diff']*100 / (df['close'] - df['diff'] )
        #df['diff_percent']=[try_float(x) for x in df['diff_percent'] ] 
        #print (df)
        if mode=='up':
            df1=df[(df.loc[:,"diff_percent"] >= 1) ].sort_values(by='diff_percent', ascending=False)
            #print (lno(),df1)
        else :
            df1=df[(df.loc[:,"diff_percent"] <= -1) ].sort_values(by='diff_percent', ascending=True)
        df1.reset_index(inplace=True)
        #print(lno(),list(df),df['name'].values.tolist())
        tmp=[]
        for i in range(0,len(df1)):
            #print(lno(),df1.at[i,'name'])
            if convert_otc_idx_list[df1.at[i,'name']]!='':
                str='{}({:.2f})'.format(convert_otc_idx_list[df1.at[i,'name']],df1.at[i,'diff_percent'])
                tmp.append(str)
        #print(lno(),list(df1),df1['name'].values.tolist())
        return tmp
        #return df1['name'].values.tolist()
    else :
        return []
if __name__ == '__main__':
    print (lno(),len(sys.argv))
    if len(sys.argv)>1 and sys.argv[1]=='d' :
        dstpath='data/inx'
        if not os.path.isdir(dstpath):
            os.makedirs(dstpath)    
        print("usage: xxx.py d =>get today ")
        print("usage: xxx.py d 20180920 =>get 0920")
        print("usage: xxx.py d 20180912 20180921 ")
        argv_len=len(sys.argv)
        if argv_len==2:
            today= datetime.today().date()
            startdate=datetime(year=today.year,month=today.month,day=today.day,)
            enddate=startdate
        elif argv_len==3:     
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=startdate
        elif argv_len==4:         
            startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
            enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
        else :
            print("fail")
            sys.exit()
        nowdatetime=startdate
        while   nowdatetime<=enddate :
            #print(lno(),Time64ToTwdate(nowdatetime))
            download_otc_sectinx(nowdatetime)
            download_tse_sectinx(nowdatetime)
            nowdatetime=nowdatetime+ relativedelta(days=1)    
            
    else :
        if len(sys.argv)==2:
            dataday=datetime.strptime(sys.argv[1],'%Y%m%d')
            print ('上市:',get_tse_power_inx(dataday,'up'))
            print ('上櫃:',get_otc_power_inx(dataday,'up'))
            print ('上市:',get_tse_power_inx(dataday,'down'))
            print ('上櫃:',get_otc_power_inx(dataday,'down'))
        else:    
            print (len(sys.argv))
            
        