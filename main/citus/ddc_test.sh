#!/bin/bash
cd ../..
bash compile.sh
mkdir citus/saved
# python3 -u feedback/feedback.py &> citus/feedback.log &
sleep 1

start=$(date +%s)
./distranger main/citus/citus_ddc_config.txt
end=$(date +%s)
elapsed=$((end - start))

echo "==== Run finished, elapsed: ${elapsed}s ===="
