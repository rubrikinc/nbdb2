#!/bin/bash -eux

echo "Stopping Read API containers"
python3.7 deploy_aws.py --target=read_api --state=down

echo "Waiting for 60 secs before starting Druid historical node"
echo "-----------------------------------------------------------"
for t in {1..6}; do echo "Sleeping ${t}0/60 seconds" && sleep 10; done

echo "Starting Read API containers"
python3.7 deploy_aws.py --target=read_api --state=up --num_instances=8
