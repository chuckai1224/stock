cd  /src/stock
. start.sh
#echo $#
if [ "$1" == "" ];then
_date=$(date +"%Y%m%d")
else
_date=$1
fi
echo date=$_date
python fut.py -d1  $_date
python revenue.py
python kline.py -d $_date $_date
python twse_big3.py -d $_date
python taiwan_dollar.py -d $_date
python pe_networth_yeild.py -d $_date
python op.py -d1 $_date
python powerinx.py -d $_date
YYYY=$(echo  $_date|cut -c 1-4)
MM=$(echo  $_date|cut -c 5-6)
DD=$(echo  $_date|cut -c 7-8)
#echo  $YYYY $MM  $DD
python crawl.py $YYYY $MM  $DD
python stock_bs_analy.py -d $_date
python big3_fut_op.py -d $_date $_date
python twii.py -d1 $_date
rem 3大法人 當日買賣股票 ./sql/stock_big3.db
python stock_big3.py -d $_date $_date
rem need data/stock_data/*
rem need 董監資料 data/dist/[mops final]
rem need 
python gg_stock.py gg $_date



