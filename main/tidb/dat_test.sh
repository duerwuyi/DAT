#!/bin/bash
cd ../..
bash compile.sh
mkdir tidb/saved
# python3 -u feedback/feedback.py &> citus/feedback.log &
sleep 1
./distranger main/tidb/tidb_ddc_config.txt
# kill $(ps aux | grep "python3 feedback.py" | awk '{print $2}')

# tiup --tag mylab playground --db 1 --pd 1 --kv 3 --tiflash 1
# tiup --tag single playground --db 1 --db.port 3307
