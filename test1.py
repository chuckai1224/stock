# -*- coding: utf-8 -*-
#from __future__ import unicode_literals
import io
import csv
import os
import time
import timeit
import sys
import pandas as pd
import numpy as np


  



def test1():

    odf=pd.DataFrame()
    odf['工號']=''
    odf['108/01']=0
    odf['108/02']=0
    odf['108/03']=0
    odf['108/04']=0
    odf['108/05']=0
    odf['108/06']=0
    odf['108/07']=0
    odf['108/08']=0
    odf['108/09']=0
    odf['108/10']=0
    odf['108/11']=0
    odf['108/12']=0
    pos=0
    df = pd.read_csv('test.csv',encoding = 'big5')
    for i in range(0,len(df)):
        r=df.iloc[i]
        
        if len (odf.loc[odf['工號'] == r['工號']])==0:
            odf.loc[r['工號']]=[r['工號'],0,0,0,0,0,0, 0,0,0,0,0,0]
            pre_cash=odf.loc[r['工號'],r['給付月']] 
            print('pre_cash:',pre_cash,r['給付月'])

            odf.loc[r['工號'],r['給付月']] =pre_cash+int(r['金額'].replace(',',''))
        else:
            print(r)
            
            pre_cash=odf.loc[r['工號'],r['給付月']] 
            print('pre_cash:',pre_cash,r['給付月'])

            odf.loc[r['工號'],r['給付月']] =pre_cash+int(r['金額'].replace(',',''))
            #print(odf)
        #print(r)
    print(odf)
    odf.to_csv('test_out.csv',encoding='utf-8', index=False)
        



    
    #print(df)
    pass


if __name__ == '__main__':
    
    test1()