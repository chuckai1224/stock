import os
from datetime import datetime
import pandas as pd
import numpy as np
import stock_comm as comm
FOLDER = '{}/stock_data'.format(comm.datafolder())

def string_to_time(string):
    if '/' in string:
        year, month, day = string.split('/')
    if '-' in string:
        year, month, day = string.split('-')   
    if int(year)>=1911:
        return datetime(int(year) , int(month), int(day))
    return datetime(int(year) + 1911, int(month), int(day))

def is_same(row1, row2):
    if not len(row1) == len(row2):
        return False

    for index in range(len(row1)):
        if row1[index] != row2[index]:
            return False
    else:
        return True
def string_float(x):
            #print(lno(),type(x))
    if type(x)==str:
        if '除息' in x :
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
def main():
    file_names = os.listdir(FOLDER)
    for file_name in file_names:
        if not file_name.endswith('.csv'):
            continue
        #
        if (len(file_name)==10):
            continue
        print(file_name,len(file_name))
        dtypes= {'vol':str, 'cash': str,'open':str, 'high': str,'low':str, 'close': str,'diff':str}    
        df=pd.read_csv('{}/{}'.format(FOLDER, file_name),encoding = 'utf-8',dtype=dtypes)
        df.columns = ['date', 'vol', 'cash', 'open', 'high','low','close','diff','Tnumber','stock_name']
        #print(df.head())
        df['date']=df['date'].apply(string_to_time)
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

        df.drop_duplicates(subset=['date'],keep='last',inplace=True)
        df=df.sort_values(by='date')
        df.to_csv('{}/{}'.format(FOLDER, file_name),encoding='utf-8', index=False)
        
        """
        dict_rows = {}
        # Load and remove duplicates (use newer)
        with open('{}/{}'.format(FOLDER, file_name), 'r', encoding='utf-8') as file:
            for line in file.readlines():
                dict_rows[line.split(',', 1)[0]] = line

        # Sort by date
        rows = [row for date, row in sorted(
            dict_rows.items(), key=lambda x: string_to_time(x[0]))]

        with open('{}/{}'.format(FOLDER, file_name), 'w', encoding='utf-8') as file:
            file.writelines(rows)
        """    

if __name__ == '__main__':
    main()
