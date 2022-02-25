#!/bin/bash -eux

echo "Stopping all services"
python3.7 deploy_aws.py --target=master_graphite_consumer --state=down
python3.7 deploy_aws.py --target=prod_no_agg_graphite_consumer --state=down
python3.7 deploy_aws.py --target=prod_graphite_consumer --state=down
python3.7 deploy_aws.py --target=v4_live_influx_consumer --state=down
python3.7 deploy_aws.py --target=internal_graphite_consumer --state=down

python3.7 deploy_aws.py --target=sparse_batch_consumer --state=down
python3.7 deploy_aws.py --target=dense_batch_consumer --state=down

python3.7 deploy_aws.py --target=read_api --state=down

python3.7 deploy_aws.py --target=druid_deployment --mode=prod --state=down \
  --master_desired_count=0 --zookeeper_desired_count=0 \
  --middlemanager_regular_desired_count=0 \
  --middlemanager_crosscluster_desired_count=0 \
  --historical_desired_count=0 \
  --query_desired_count=0 \
  --query_cc_desired_count=0
