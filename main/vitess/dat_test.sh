#!/bin/bash
cd ../..
bash compile.sh
mkdir vitess/saved
# python3 -u feedback/feedback.py &> citus/feedback.log &
sleep 1
./distranger main/vitess/vitess_ddc_config.txt

#duerwuyi/vitess-vtgate:routecount-v2
