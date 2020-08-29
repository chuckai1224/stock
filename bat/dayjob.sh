echo week job Get file from ::  https://data.gov.tw/dataset/11452 and save to stock/download/TDCC_OD_1-5.csv
#echo tdcc_dist.py 1 ==>update tdcc date
#echo tdcc_dist.py 2 ==>update tdcc data
python3 kline.py -d 
python3 twse_big3.py 
python3 taiwan_dollar.py 
python3 op.py 
python3 powerinx.py d
python3 crawl.py 
python3 stock_bs_analy.py
python3 big3_fut_op.py
python3 twii.py
python3 stock_big3.py



