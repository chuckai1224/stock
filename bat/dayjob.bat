echo week job Get file from ::  https://data.gov.tw/dataset/11452 and save to stock/download/TDCC_OD_1-5.csv
#echo tdcc_dist.py 1 ==>update tdcc date
#echo tdcc_dist.py 2 ==>update tdcc data
python fut.py  
python revenue.py
python kline.py -d 
python twse_big3.py 
python taiwan_dollar.py 
python pe_networth_yeild.py
python op.py 
python powerinx.py d
python crawl.py 
python stock_bs_analy.py
python big3_fut_op.py
python twii.py
python stock_big3.py
python gg_stock.py



