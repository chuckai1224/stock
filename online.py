import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
from flask import Flask,render_template,redirect, url_for

import numpy as np
import pandas as pd
import inspect
from inspect import currentframe, getframeinfo
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)

app = Flask(__name__)

@app.route('/')
def hello_world():
  print (test111)
  return 'Hello World!'

@app.route('/online/')
#@app.route('/test/<name>')
def online():
  print (test111)  
  return render_template('online.html')
@app.route('/online/top')
def top():
    return render_template('top.html')
@app.route('/online/kline')
def kline():
    return render_template('kline.html')
@app.route("/online/", methods=['POST'])
def move_forward():
    #Moving forward code
    forward_message = "Moving Forward..."
    print (lno(),forward_message)
    return redirect(url_for('online'))

def get_tse_df_from_csv(enddate):
    dstpath='csv/tse.csv'
    df = pd.read_csv(dstpath,encoding = 'big5',skiprows=1)
    df.dropna(axis=1,how='all',inplace=True)
    df.dropna(inplace=True)
    df.columns = ['date','open','high','low','close','MA5','MA10','MA20','MA60','vol','MA5-','MA10-','DIF12-26','MACD9','OSC','K9','D9']
    df['diff'] = df['close'] - df['close'].shift(1)
    ## date str type YYYY/MM/DD
    df['date'] = df['date'].astype('datetime64[ns]')
    #print (lno(),df.tail(3), np.datetime64(enddate),df.loc[1,"date"])
    df=df[df.loc[:,"date"] <= np.datetime64(enddate)]
    print (lno(),df.tail(3))
    return df
   

   
    
if __name__ == '__main__':
  #df=kline.get_tse_kline_df(startdate,enddate)
  if sys.argv[1]=='1' :
    startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
    enddate=datetime.strptime(sys.argv[3],'%Y%m%d')
    selectdate=startdate
    df=get_tse_df_from_csv(enddate)  
   
  test111='tess'
  app.run(debug=True)