cd  /src/stock
. start.sh
echo get eps
python eps.py now
echo use xq get 季研發費用 
## parse xq_data/rr to sql
python xq.py now

