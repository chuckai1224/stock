# -*- coding: utf-8 -*-
cimport numpy as np
from numpy import nan
from cython import boundscheck, wraparound
#import grs
import csv
import os
import sys
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

cdef extern from "numpy/arrayobject.h":
    int PyArray_TYPE(np.ndarray)
    np.ndarray PyArray_EMPTY(int, np.npy_intp*, int, int)
    int PyArray_FLAGS(np.ndarray)
    np.ndarray PyArray_GETCONTIGUOUS(np.ndarray)
ctypedef int TA_RetCode

np.import_array()
cdef double NaN = nan
cdef np.ndarray check_array(np.ndarray real):
    if PyArray_TYPE(real) != np.NPY_DOUBLE:
        raise Exception("input array type is not double")
    if real.ndim != 1:
        raise Exception("input array has wrong dimensions")
    if not (PyArray_FLAGS(real) & np.NPY_C_CONTIGUOUS):
        real = PyArray_GETCONTIGUOUS(real)
    return real
cdef np.npy_intp check_length2(np.ndarray a1, np.ndarray a2) except -1:
    cdef:
        np.npy_intp length
    length = a1.shape[0]
    if length != a2.shape[0]:
        raise Exception("input array lengths are different")
    return length

cdef np.npy_intp check_length3(np.ndarray a1, np.ndarray a2, np.ndarray a3) except -1:
    cdef:
        np.npy_intp length
    length = a1.shape[0]
    if length != a2.shape[0]:
        raise Exception("input array lengths are different")
    if length != a3.shape[0]:
        raise Exception("input array lengths are different")
    return length
cdef np.npy_intp check_length4(np.ndarray a1, np.ndarray a2, np.ndarray a3, np.ndarray a4) except -1:
    cdef:
        np.npy_intp length
    length = a1.shape[0]
    if length != a2.shape[0]:
        raise Exception("input array lengths are different")
    if length != a3.shape[0]:
        raise Exception("input array lengths are different")
    if length != a4.shape[0]:
        raise Exception("input array lengths are different")
    return length

cdef np.npy_int check_begidx1(np.npy_intp length, double* a1) except -1:
    cdef:
        double val
    for i from 0 <= i < length:
        val = a1[i]
        if val != val:
            continue
        return i
    else:
        raise Exception("inputs are all NaN")

cdef np.npy_int check_begidx2(np.npy_intp length, double* a1, double* a2) except -1:
    cdef:
        double val
    for i from 0 <= i < length:
        val = a1[i]
        if val != val:
            continue
        val = a2[i]
        if val != val:
            continue
        return i
    else:
        raise Exception("inputs are all NaN")

cdef np.npy_int check_begidx3(np.npy_intp length, double* a1, double* a2, double* a3) except -1:
    cdef:
        double val
    for i from 0 <= i < length:
        val = a1[i]
        if val != val:
            continue
        val = a2[i]
        if val != val:
            continue
        val = a3[i]
        if val != val:
            continue
        return i
    else:
        raise Exception("inputs are all NaN")

cdef np.npy_int check_begidx4(np.npy_intp length, double* a1, double* a2, double* a3, double* a4) except -1:
    cdef:
        double val
    for i from 0 <= i < length:
        val = a1[i]
        if val != val:
            continue
        val = a2[i]
        if val != val:
            continue
        val = a3[i]
        if val != val:
            continue
        val = a4[i]
        if val != val:
            continue
        return i
    else:
        raise Exception("inputs are all NaN")

cdef np.ndarray make_double_array(np.npy_intp length, int lookback):
    cdef:
        np.ndarray outreal
        double* outreal_data
    outreal = PyArray_EMPTY(1, &length, np.NPY_DOUBLE, np.NPY_DEFAULT)
    outreal_data = <double*>outreal.data
    for i from 0 <= i < min(lookback, length):
        outreal_data[i] = NaN
    return outreal

cdef np.ndarray make_int_array(np.npy_intp length, int lookback):
    cdef:
        np.ndarray outinteger
        int* outinteger_data
    outinteger = PyArray_EMPTY(1, &length, np.NPY_INT32, np.NPY_DEFAULT)
    outinteger_data = <int*>outinteger.data
    for i from 0 <= i < min(lookback, length):
        outinteger_data[i] = 0
    return outinteger    

cpdef double calc_profit(double buy_price,double sell_price):
    cdef double transaction_tax=0.001425*0.5
    cdef double tax_payment=0.003
    cdef double fix_buy_price=buy_price*(1+transaction_tax)
    cdef double fix_sell_price=sell_price*(1-tax_payment-transaction_tax)
    return fix_sell_price/fix_buy_price
cpdef check_long_kline_pre(float open,float high,float low,float close,float diff):
    if diff<0:
        return 0
    cdef float pre_close=close-diff
    # 计算上影线长度
    cdef upper_shadow=high-max(close,open)
    # 计算下影线长度
    cdef lower_shadow=min(close,open)-low
    # 计算实体长度
    cdef real_body=abs(close-open)
    if diff>0 and open >pre_close:
        real_body= real_body+open-pre_close
    elif diff<0 and open <pre_close:
        real_body= real_body+pre_close-open
    ratio=  real_body/pre_close  
    if ratio<0.03:
        return 0
    ratio=  upper_shadow/real_body
    if ratio>=0.03:
        return 0
    return 1

@wraparound(False)  # turn off relative indexing from end of lists
@boundscheck(False) # turn off bounds-checking for entire function


def check_long_candle( np.ndarray open not None , np.ndarray high not None , np.ndarray low not None , np.ndarray close not None, np.ndarray diff not None,int loop_back_date ):
    """ CDLSPINNINGTOP(open, high, low, close)
    Spinning Top (Pattern Recognition)
    Inputs:
        prices: ['open', 'high', 'low', 'close','diff']
    Outputs:
        integer (values are -100, 0 or 100)
    """
    cdef:
        np.npy_intp length
        int begidx, endidx, lookback
        TA_RetCode retCode
        int outbegidx
        int outnbelement
        np.ndarray outbouble
        double sum=0,real_body=0,now_real_body=0,box_high,box_low
    open = check_array(open)
    high = check_array(high)
    low = check_array(low)
    close = check_array(close)
    length = check_length4(open, high, low, close)
    begidx = check_begidx4(length, <double*>(open.data), <double*>(high.data), <double*>(low.data), <double*>(close.data))
    endidx = <int>length - begidx - 1
    outbouble = make_double_array(length, length)
    #print('test',begidx,endidx,length)
    cnt=0
    box_high=high[endidx-loop_back_date]
    box_low=low[endidx-loop_back_date]
    for i in range(endidx-loop_back_date,endidx+1):
        if diff[i]>0 and open[i] >(close[i]-diff[i]):
            #outbouble[i]= diff[i]
            if i==endidx:
                now_real_body= diff[i]
            else:
                real_body= diff[i]
                
        elif diff[i]<0 and open[i] <(close[i]-diff[i]):
            #outbouble[i]= -diff[i]
            if i==endidx:
                now_real_body= -diff[i]
            else:
                real_body= -diff[i]
        else:
            #outbouble[i]=abs(close[i]-open[i])
            if i==endidx:
                now_real_body= abs(close[i]-open[i])
            else:
                real_body= abs(close[i]-open[i])
        if i!=endidx:        
            sum=sum+real_body 
            #print(cnt,real_body,sum)   
            cnt=cnt+1
            if high[i]>box_high:
                box_high=high[i]
            if low[i]<box_low:
                box_low=low[i]
        #print(outbouble[i])
    #np.sort(lst)
    #print(cnt,now_real_body,sum/loop_back_date)
    if sum==0:
        return 0,box_high,box_low
    return now_real_body/(sum/loop_back_date),box_high,box_low

def get_long_red_ratio( np.ndarray open not None , np.ndarray high not None , np.ndarray low not None , np.ndarray close not None, np.ndarray diff not None,int loop_back_date ):
    """ CDLSPINNINGTOP(open, high, low, close)
    Spinning Top (Pattern Recognition)
    Inputs:
        prices: ['open', 'high', 'low', 'close','diff']
    Outputs:
        integer (values are -100, 0 or 100)
    """
    cdef:
        np.npy_intp length
        int begidx, endidx, lookback
        TA_RetCode retCode
        int outbegidx
        int outnbelement
        np.ndarray outbouble
        double sum=0,real_body=0,now_real_body=0,box_high,box_low
    open = check_array(open)
    high = check_array(high)
    low = check_array(low)
    close = check_array(close)
    length = check_length4(open, high, low, close)
    begidx = check_begidx4(length, <double*>(open.data), <double*>(high.data), <double*>(low.data), <double*>(close.data))
    endidx = <int>length - begidx - 1
    outbouble = make_double_array(length, length)
    #print('test',begidx,endidx,length)
    cnt=0
    box_high=high[endidx-loop_back_date]
    box_low=low[endidx-loop_back_date]
    for i in range(endidx-loop_back_date,endidx+1):
        if i==endidx:
            now_real_body= abs(close[i]-open[i])
        else:
            real_body= abs(close[i]-open[i])
        if i!=endidx:        
            sum=sum+real_body 
            #print(cnt,real_body,sum)   
            cnt=cnt+1
            if high[i]>box_high:
                box_high=high[i]
            if low[i]<box_low:
                box_low=low[i]
        #print(outbouble[i])
    #np.sort(lst)
    #print(cnt,now_real_body,sum/loop_back_date)
    if sum==0:
        return 0,box_high,box_low
    return now_real_body/(sum/loop_back_date),box_high,box_low

