#!/bin/bash
cd ../..
bash compile.sh
mkdir ndb/saved
# python3 -u feedback/feedback.py &> citus/feedback.log &
sleep 1
./distranger main/ndb/ndb_ddc_config.txt
