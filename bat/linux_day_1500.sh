cd  /src/stock
. start.sh

python kline.py -d 



python twse_big3.py 

python taiwan_dollar.py 

python op.py 
echo download tse otc 主流/data/inx
python powerinx.py d

echo download 三大法人 未平倉口數 契約金額 csv/fut_op_big3/
python big3_fut_op.py
#echo download 大盤 價格 量 ./csv/twii/twii_data.csv./csv/twii
python crawl.py 
python stock_bs_analy.py

python twii.py
python fut.py






