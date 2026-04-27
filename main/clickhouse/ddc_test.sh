#!/bin/bash
cd ../..
bash compile.sh
mkdir clickhouse/saved
# python3 -u feedback/feedback.py &> citus/feedback.log &
sleep 1
./distranger main/clickhouse/clickhouse_ddc_config.txt
# kill $(ps aux | grep "python3 feedback.py" | awk '{print $2}')
