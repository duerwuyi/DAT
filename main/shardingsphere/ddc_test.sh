#!/bin/bash
cd ../..
bash compile.sh
mkdir shardingsphere/saved
# python3 -u feedback/feedback.py &> citus/feedback.log &
sleep 1
./distranger main/shardingsphere/ss_ddc_config.txt
# kill $(ps aux | grep "python3 feedback.py" | awk '{print $2}')
