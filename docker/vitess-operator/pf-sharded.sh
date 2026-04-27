#!/bin/sh

kubectl port-forward -n vitess-sharded --address localhost "$(kubectl get service -n vitess-sharded --selector="planetscale.com/component=vtctld" -o name | head -n1)" 25000:15000 25999:15999 &
process_id1=$!
kubectl port-forward -n vitess-sharded --address localhost "$(kubectl get service -n vitess-sharded --selector="planetscale.com/component=vtgate,!planetscale.com/cell" -o name | head -n1)" 25306:3306 &
process_id2=$!
kubectl port-forward -n vitess-sharded --address localhost "$(kubectl get service -n vitess-sharded --selector="planetscale.com/component=vtadmin" -o name | head -n1)" 24000:15000 24001:15001 &
process_id3=$!
sleep 2
echo "You may point your browser to http://localhost:25000, use the following aliases as shortcuts:"
echo 'alias vtctldclient="vtctldclient --server=localhost:25999 --logtostderr"'
echo 'alias mysql="mysql -h 127.0.0.1 -P 25306 -u user"'
echo "Hit Ctrl-C to stop the port forwards"
wait $process_id1
wait $process_id2
wait $process_id3
