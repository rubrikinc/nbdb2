#!/bin/bash -eux

echo "Rebuilding AnomalyDB docker images"
python3.7 deploy_aws.py --target=druid_historical --mode=prod --state=build

echo "Stopping old Druid historical node"
terraform apply -auto-approve \
  -target=module.prod_druid_deployment.aws_ecs_service.druid_historical \
  -var master_desired_count=1 \
  -var zookeeper_desired_count=3 \
  -var middlemanager_regular_desired_count=1 \
  -var middlemanager_crosscluster_desired_count=1 \
  -var historical_desired_count=0 \
  -var query_desired_count=1 \
  -var query_cc_desired_count=1

echo "Waiting for 1 mins before starting Druid historical node"
echo "-----------------------------------------------------------"
for t in {1..6}; do echo "Sleeping ${t}0/60 seconds" && sleep 10; done

echo "Starting Druid historical node"
terraform apply -auto-approve \
  -target=module.prod_druid_deployment.aws_ecs_service.druid_historical \
  -var master_desired_count=1 \
  -var zookeeper_desired_count=3 \
  -var middlemanager_regular_desired_count=1 \
  -var middlemanager_crosscluster_desired_count=1 \
  -var historical_desired_count=49 \
  -var query_desired_count=1 \
  -var query_cc_desired_count=1
