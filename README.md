## psrs/psr 三年低  -1  (-2 << 2)

if 0.2-x >2
  y=2
elif 0.2-x <-2  
  y=-2
else
    y=(0.2-x)*2

## 淨值比 <1.3
(1.3 -x )
if x>=3.3 
    y=-2
elif x>=1.3      
    y=1.3-x
else
    y=(1.3-x)*2

## psrs <0.75 (-4  ~ 4)
if psrs>=3
    y=-4
else
    y=(1.5-psrs)/0.375
     

## prr<15 
if prr>=15
    y=-2
elif prr<=0
    y=-2    
else 
    y=(15-prr)/7.5

## 毛利率 >30 (-2 ~2)
if x<10
    y=-2
elif x>=60    
    y=2
else
    y=(x-30)/15    

## 營利率年增 (-2 ~2)
x=本季營利率-去年同期營利率
if x/2.5<-2
   y=-2
elif x/2.5>2
    y=2
else
    y=x/2.5    

## 營收年增 20%(-2 ~2)
x=最新月營收年成長率
w=本年累計營收年成長率-去年營收年增率
if (x*100-10)*0.25/10+w*0.25>2
    y=2
else
    y=(x*100-10)*0.25/10+w*0.25   
## 營收月增 80%(-2 ~2)
if x>0.8
    y=2
elif x<-0.8
    y=-2    
else
    y=  x*2.5  
## peg
今年eps預估(最新一季eps*2+前一季eps+前2季eps)
(eps預估年成長率)a= (最新一季eps*2+前一季eps+前2季eps)/(去年同季eps*2+去年前一季eps+去年前2季eps) 
if a<0
    peg=1.34
else
    if 本益比=NA
        if 今年eps預估<0
            peg=1.34
        else
            peg=股價/今年eps預估/eps預估年成長率/100
    else        
        peg=本益比/(eps預估年成長率)/100
if peg>1.34
    y=-2
elif peg <0.66
    y=2
else
    y=(1-peg)*2/0.34        

http://lovecoding.logdown.com/

外資月 call delta=0.713386
外資月 put delta =-0.921187
十大 月 buy call 口數=66357	
十大 月 buy put 口數=98918
十大約當大台=外資月 call delta* 十大 月 buy call 口數 +外資月 put delta* 十大 月 buy put 口數 
# stock
## 外資
1. 期貨 
預估未來,
最低點之前,就先建立多單,容易虧損一段時間需配合選擇權

2. op 
>買方
口數 :方向性不大,多口數 通常都是避險(小點數)
契約金額: 配合期貨判斷方向

>賣方
口數: 
契約金額:
 漲多做壓力
 高檔區 為盤整 機率高
 賣方不太有方向性:
總結 大台+小台+op約當大台 為方向,參考賣方 契約 看支撐 壓力

## 自營商
1.期貨
2.op買方
>口數:
>契約金額:
3.op賣方
>口數: 下跌段sell call(做壓力) > sell put(做支撐)
>契約金額: 
賣方的口數 跟  契約金額抓轉折


## 十大交易人
1. 期貨
    轉倉模式
    近月減少,所有增加(有機會壓低結算,要觀察 所有 是否增加)
2. op
>周選模式:
十大交易人op buy call ,buy put
top 10 op only have num , no cash

周選 是拿來避險的
周 buy call > 周 buy put ==>方向明確,暴衝
                        ==> 如回檔時出現,再走一波機率高
周 buy put > 周 buy call ==> 如為期指價格漲高,需參考 十大期貨多單,為保險單

周 sell call 壓力(用綠色)
周 sell put  支撐 (用紅色)

賣方 是莊家
# 期貨 > 周賣>周買
>月選模式:
大部分基金都操做月選,周選為避險
# 月選 buy 比周選 buy 更有方向性
再沒有契約金額的狀況下
賣方的參考性 比 買方高
>所有模式:
買方:
下跌轉折,buy call >buy put ,漲高之後 以避險為主
當看好未來行情,有可能出現先布局
賣方:
當空方時,不做支撐,只做壓力
當破底時,出現sell put(做支撐)>sell call (做壓力)

買方一定要參考 外資+自營商 的契約金額
十大交易人=外資+自營商
可以參考外資+自營商 的契約金額

## 每日多空指標 
>0 =1 <
外資約當大台 >0:1 <0:-1
外資買賣權金額 (buy call cash -buy put cash) /(buy call cash +buy put cash)
自營商約當大台 >0:1 <0:-1
自營商賣方口數 (sell put num -sell call num)/(sell put num +sell call num)
十大交易人約當大台 >0:1 <0:-1
十大交易人近月(W)賣方口數 (sell put num -sell call num)/(sell put num +sell call num)
十大交易人遠月(M)賣方口數 (sell put num -sell call num)/(sell put num +sell call num)


