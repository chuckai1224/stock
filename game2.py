# -*- coding: utf-8 -*-
import os
import time
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pygame
from pygame.locals import *
import kline
import csv
import numpy as np
import pandas as pd
from math import ceil
from pyecharts import EffectScatter
import inspect
from inspect import currentframe, getframeinfo
def lno():
    cf = currentframe()
    filename = getframeinfo(cf).filename
    return '%s-L(%d)'%(os.path.basename(filename),inspect.currentframe().f_back.f_lineno)

def show_text(screen,font,text, x, y):
#專門顯示文字的方法，除了顯示文字還能指定顯示的位置
    x = x
    y = y
    text = font.render(text, True, (255, 255, 255))
    screen.blit(text, (x, y))
    pygame.display.update()
def resample(in_df,period,nums):
    # https://pandas-docs.github.io/pandas-docs-travis/timeseries.html#offset-aliases
    # 周 W、月 M、季度 Q、10天 10D、2周 2W
    #period = 'W'
    
    df=in_df.set_index('date')
    period_df = df.resample(period).last()
    period_df['open'] = df['open'].resample(period).first()
    period_df['high'] = df['high'].resample(period).max()
    period_df['low'] = df['low'].resample(period).min()
    period_df['close'] = df['close'].resample(period).last()
    period_df['vol'] = df['vol'].resample(period).sum()
    #period_df['amount'] = df['amount'].resample(period, how='sum')
    # 去除空的数据（没有交易的周）
    period_df = period_df.dropna(how='any',axis=0)
    period_df.reset_index(inplace=True)
    period_df_new = period_df.tail(nums) 
    return period_df_new

def generate_kline_picture(df,nowdatetime,enddate):
    pic_out='pic'
    if not os.path.isdir(pic_out):
        os.makedirs(pic_out)     
    nowdatetime=nowdatetime+ relativedelta(days=1)    
    while   nowdatetime<=enddate :
        #print lno(),nowdatetime,df[df.loc[:,"date"] == np.datetime64(nowdatetime)]
        
        if len(df[df.loc[:,"date"] == np.datetime64(nowdatetime)]) == 0 :
            #print lno(),nowdatetime,"no data"
            nowdatetime=nowdatetime+ relativedelta(days=1)
            
            continue
        pic_file="%s/%d%02d%02d_tse_kline.gif"%(pic_out,nowdatetime.year,nowdatetime.month,nowdatetime.day)  
        print (lno(),pic_file) 
        if os.path.isfile(pic_file):
            print ('{}file exist {}'.format(lno(),pic_file))
        else :     
            sample_df=df[df.loc[:,"date"] <= np.datetime64(nowdatetime)]
            """
            week_df=resample(sample_df,'W',48)
            mon_df=resample(sample_df,'M',24)
            #print lno(),week_df
            df_new1 = sample_df.tail(120)    
            df_new_sort = df_new1.sort_values('date', ascending=True)
            kline.kline_js_mode1('日K',df_new_sort,week_df,mon_df,list(df_new_sort),'ma10',500,400,'top',pic_file)
            """
            df_new1 = sample_df.tail(250)
            df_new_sort=df_new1.sort_values('date', ascending=True)
            df_new_sort=df_new_sort.reset_index(drop=True)    
            df_new_sort = kline.get_stock_indexs_df(df_new_sort)
            kline.tse_kline_js('日K',df_new_sort,list(df_new_sort),pic_file,None) 
            #"""
        img = pygame.image.load(pic_file)
        #img=pygame.transform.scale(img, (1280, 800))
       
        return nowdatetime,img
def week_of_month(dt):
    """ Returns the week of the month for the specified date.
    """
    first_day = dt.replace(day=1)
    dom = dt.day
    adjusted_dom = dom + first_day.weekday()
    if first_day.weekday()>=3:
        return int(ceil(adjusted_dom/7.0)) -1
    return int(ceil(adjusted_dom/7.0))               
def save_trade_history(buy_date,buy_fin_date,sell_date,sell_fin_date):
    filename='csv/trade_history.csv'  
    tmp=[]
    tmp.append(buy_date)  
    tmp.append(buy_fin_date)  
    tmp.append(sell_date)  
    tmp.append(sell_fin_date)  
    with open(filename, "w") as output:
        # A chunk of 128 bytes
        writer = csv.writer(output, lineterminator='\n')
        writer.writerows(tmp)
          
def genetate_trade_history_html(tag,df,nowdate,begin_date,end_date):
    ypos_b=[]
    ypos_e=[]
    xpos_b=[]
    xpos_e=[]
    es = EffectScatter("買賣點標示")
    sel_df=df[df.loc[:,"date"] <= nowdate].tail(250).set_index('date')
    if (len(begin_date)==0):
        return
    #print (lno(),sel_df)
    #print (lno(),sel_df.head())
    if tag=='buy':
        prev_date=begin_date[0]
        cnt=0
        for i in begin_date:
            try:
                y_pos=sel_df.at[i,'low']
            except:
                print (lno(),i)    
                continue
            if prev_date== i:
                y_pos=y_pos-cnt*30
                cnt=cnt+1
            else :
                cnt=0    
            prev_date=i    
            ypos_b.append(y_pos)
            xpos_b.append(kline.time64_date(i))

        prev_date=end_date[0]
        cnt=0
        for i in end_date:
            try:
                y_pos=sel_df.at[i,'high']
            except:
                print (lno(),i)    
                continue        
            if prev_date== i:
                y_pos=y_pos+cnt*30
                cnt=cnt+1
            else :
                cnt=0    
            prev_date=i
            ypos_e.append(y_pos)
            xpos_e.append(kline.time64_date(i))
            year=str(i)[:4]    
        es.add("",xpos_b, ypos_b, symbol_size=8, effect_scale=1.5,effect_period=2, symbol="triangle")  
        es.add("",xpos_e, ypos_e, symbol_size=8, effect_scale=1.5,effect_period=2, symbol="diamond")
    else :
        prev_date=begin_date[0]
        cnt=0
        for i in begin_date:
            try:
                y_pos=sel_df.at[i,'high']
            except:
                print (lno(),i)    
                continue            
            if prev_date== i:
                y_pos=y_pos+cnt*30
                cnt=cnt+1
            else :
                cnt=0    
            prev_date=i    
            ypos_b.append(y_pos)
            xpos_b.append(kline.time64_date(i))

        prev_date=end_date[0]
        cnt=0
        for i in end_date:
            try:
                y_pos=sel_df.at[i,'low']
            except:
                print (lno(),i)    
                continue            
            if prev_date== i:
                y_pos=y_pos+cnt*30
                cnt=cnt+1
            else :
                cnt=0    
            prev_date=i
            ypos_e.append(y_pos)
            xpos_e.append(kline.time64_date(i))
            year=str(i)[:4]
        es.add("",xpos_b, ypos_b, symbol_size=8, effect_scale=1.5,effect_period=2, symbol="pin")  
        es.add("",xpos_e, ypos_e, symbol_size=8, effect_scale=1.5,effect_period=2, symbol="diamond")           
    
    sel_df.reset_index(inplace=True)
    now_str=datetime.now().strftime('%Y%m%d')
    filename='out/{0}_{1}_{2}.html'.format(nowdate.strftime('%Y%m%d'),tag,now_str)
    sel_df=kline.get_stock_indexs_df(sel_df)
    kline.tse_kline_js('日K',sel_df,list(sel_df),filename,es)     
    print (lno(),ypos_e)

    
def main1(startdate,enddate):
    path='out'
    if not os.path.isdir(path):
            os.mkdir(path)
    pygame.init()   
    df=kline.get_tse_kline_df(startdate,enddate)
    #week_df=resample(df,'W')
    #print lno(),week_df
    #mon_df=resample(df,'M')
    #print lno(),mon_df
    money=1000000
    buy_list=[]
    buy_date=[]
    buy_fin_date=[]
    sell_list=[]
    sell_date=[]
    sell_fin_date=[]
    version = " v0.0.0"
    pygame.display.set_caption("K Line" + version)

    clock = pygame.time.Clock()

    screen = pygame.display.set_mode((1280, 900))
    pic_rect=pygame.Rect(0, 0, 1280, 800) 
    buy_button = pygame.Rect(1100, 800, 50, 50) 
    buy_fin_button = pygame.Rect(1150, 800, 50, 50) 
    sell_button = pygame.Rect(1100, 850, 50, 50) 
    sell_fin_button = pygame.Rect(1150, 850, 50, 50) 
    money_rect = pygame.Rect(400, 825, 100, 50) 
    date_rect= pygame.Rect(550, 825, 250, 50)
     

    background = pygame.Surface(screen.get_size())
    background = background.convert()
    background.fill((125, 125, 125))
    screen.blit(background, (0, 0))

    font = pygame.font.Font("./Fonts/mingliu.ttc", 24)#本文主角
    #img = pygame.image.load('clouds.bmp')
    pygame.display.update()

    running = True
    run_opening = True
    nowdatetime=startdate
    day=0
    cur_year=0
    while running:
        
        clock.tick(30)
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            if event.type == KEYDOWN:
                if event.key == K_RETURN:
                    nowdatetime,img=generate_kline_picture(df,nowdatetime,enddate)
                    #img = pygame.image.load('clouds.bmp')
                    screen.blit(img,(0,0))
                    pygame.display.update()  
                    print (lno(),"key down")
            if event.type == MOUSEBUTTONDOWN:
                
                mouse_pos = event.pos
                """
                x,y = pygame.mouse.get_pos()
                print lno(),x,y        
                if x>=0 and x<=1000  and y>=0 and y<=900:
                    nowdatetime,img=generate_kline_picture(df,nowdatetime,enddate)
                    screen.blit(img,(0,0))
                """
                if pic_rect.collidepoint(mouse_pos):
                    nowdatetime,img=generate_kline_picture(df,nowdatetime,enddate)
                    screen.blit(img,(0,0))
                    if cur_year==0:
                        cur_year=nowdatetime.year
                    elif cur_year!= nowdatetime.year :
                        cur_year=nowdatetime.year
                        #genetate_trade_history_html('buy',df,nowdatetime,buy_date,buy_fin_date)
                        #genetate_trade_history_html('sell',df,nowdatetime,sell_date,sell_fin_date)    
                if buy_button.collidepoint(mouse_pos):
                    
                    #buy_price=df[df['date'] == nowdatetime].loc[:, ['date','close']].values.tolist()
                    #print lno(),df[df['date'] == nowdatetime].loc[:, ['close']],buy_price
                    money=money-20000
                    buy_list.append(df.loc[(df.date==nowdatetime),'close'].values[0])
                    buy_date.append(df.loc[(df.date==nowdatetime),'date'].values[0])
                    print (lno(),buy_list)
                    # prints current location of mouse
                    print('red button was pressed at {0}'.format(mouse_pos))
                    save_trade_history(buy_date,buy_fin_date,sell_date,sell_fin_date)
                if sell_button.collidepoint(mouse_pos):
                    # prints current location of mouse
                    money=money-20000
                    sell_list.append(df.loc[(df.date==nowdatetime),'close'].values[0])
                    sell_date.append(df.loc[(df.date==nowdatetime),'date'].values[0])
                    print('green button was pressed at {0}'.format(mouse_pos))
                    save_trade_history(buy_date,buy_fin_date,sell_date,sell_fin_date) 
                if buy_fin_button.collidepoint(mouse_pos):
                    # prints current location of mouse
                    if len(buy_list)>0 :
                        buy_fin_price=df.loc[(df.date==nowdatetime),'close'].values[0]
                        org_buy_price=buy_list.pop(0)
                        money=int(money+20000+ (buy_fin_price-org_buy_price-2)*50 )
                        buy_fin_date.append(df.loc[(df.date==nowdatetime),'date'].values[0])
                        print (lno(),money)
                        save_trade_history(buy_date,buy_fin_date,sell_date,sell_fin_date)
                        #genetate_trade_history_htmlgenetate_trade_history_html('buy',df,nowdatetime,buy_date,buy_fin_date)
                    print('buy_fin_button was pressed at {0}'.format(mouse_pos))     
                if sell_fin_button.collidepoint(mouse_pos):
                    # prints current location of mouse
                    if len(sell_list)>0 :
                        sell_fin_price=df.loc[(df.date==nowdatetime),'close'].values[0]
                        org_sell_price=sell_list.pop(0)
                        money=int(money+20000+ (org_sell_price-sell_fin_price-2)*50 )
                        sell_fin_date.append(df.loc[(df.date==nowdatetime),'date'].values[0])
                        print (lno(),money)
                        save_trade_history(buy_date,buy_fin_date,sell_date,sell_fin_date)
                        #genetate_trade_history_html('sell',df,nowdatetime,sell_date,sell_fin_date)
                    print('buy_fin_button was pressed at {0}'.format(mouse_pos))       
    
                
            
        if run_opening:
            #op_background = pygame.Surface(screen.get_size())
            #op_background = op_background.convert()
            #op_background.fill((0, 0, 0))
            #screen.blit(op_background, (0, 0))
            #print lno(),"ttt"
            
            text = '%d-%02d-%02d (%dweek-%d)'%(nowdatetime.year,nowdatetime.month,nowdatetime.day,week_of_month(nowdatetime),nowdatetime.weekday()+1)
            if repeat==1:
                nowdatetime,img=generate_kline_picture(df,nowdatetime,enddate)
            pygame.draw.rect(screen,(125,125,125),date_rect)
            show_text(screen,font,text, date_rect.left, date_rect.top)

            pygame.draw.rect(screen,(125,125,125),money_rect)
            show_text(screen,font,str(money), money_rect.left, money_rect.top)

            pygame.draw.rect(screen, [255, 0, 0], buy_button)
            show_text(screen,font,"BUY", buy_button.left, buy_button.top)#使用show_text()
            pygame.draw.rect(screen, [0, 0, 255], buy_fin_button)

            pygame.draw.rect(screen, [0, 255, 0], sell_button)
            show_text(screen,font,"SELL", sell_button.left, sell_button.top)#使用show_text()

            pygame.draw.rect(screen, [0, 0, 200], sell_fin_button)
            
            
            pygame.display.update()
            
    pygame.quit()
##pyinstaller game.py --hidden-import=pandas._libs.tslibs.np_datetime --hidden-import=pandas._libs.tslibs.nattype --hidden-import=pandas._libs.skiplist


if __name__ == '__main__':
    repeat=0
    if len(sys.argv)==1:
        startdate=datetime.strptime('20110101','%Y%m%d')
        enddate=datetime.strptime('20170713','%Y%m%d')     
        main1(startdate,enddate)
    elif sys.argv[1]=='1' :
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        enddate=datetime.strptime(sys.argv[3],'%Y%m%d')     
        main1(startdate,enddate)
    elif sys.argv[1]=='2' :
        startdate=datetime.strptime(sys.argv[2],'%Y%m%d')
        enddate=datetime.strptime(sys.argv[3],'%Y%m%d') 
        repeat=1    
        main1(startdate,enddate) 
    
