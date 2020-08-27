H:
cd H:\Python36-32\stock 

curl https://hk.finance.yahoo.com/quote/NFLX/history?p=NFLX -o final/NFLX.htm
rem curl https://finance.yahoo.com/quote/NFLX/history?p=NFLX -o final/NFLX.htm
curl https://hk.finance.yahoo.com/quote/DX-Y.NYB/history?p=DX-Y.NYB -o final/DX-Y.NYB.htm
curl https://hk.finance.yahoo.com/quote/FB/history?p=FB -o final/FB.htm
curl https://hk.finance.yahoo.com/quote/AMZN/history?p=AMZN  -o final/AMZN.htm
curl https://hk.finance.yahoo.com/quote/GOOG/history?p=GOOG  -o final/GOOD.htm
curl https://hk.investing.com/rates-bonds/u.s.-10-year-bond-yield-historical-data --user-agent "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36" -o final/us10y-bond.htm
python morning.py
conda activate myenv & python morning.py & bat\update_final.bat 
