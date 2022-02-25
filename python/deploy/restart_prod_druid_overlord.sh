#!/bin/bash -eux

echo "Rebuilding AnomalyDB docker images"
python3.7 deploy_aws.py --target=druid_master --mode=prod --state=build

echo "Stopping old Druid master services"
terraform apply -auto-approve \
  -target=module.prod_druid_deployment.aws_ecs_service.druid_master_overlord \
  -var master_desired_count=0 \
  -var zookeeper_desired_count=3 \
  -var middlemanager_regular_desired_count=1 \
  -var middlemanager_crosscluster_desired_count=1 \
  -var historical_desired_count=3 \
  -var query_desired_count=1 \
  -var query_cc_desired_count=1

echo "Waiting for 3 mins before starting Druid master services"
echo "-----------------------------------------------------------"
for t in {1..3}; do echo "Sleeping $t/3 mins" && sleep 60; done

echo "Starting Druid master services"
terraform apply -auto-approve \
  -target=module.prod_druid_deployment.aws_ecs_service.druid_master_overlord \
  -var master_desired_count=3 \
  -var zookeeper_desired_count=3 \
  -var middlemanager_regular_desired_count=1 \
  -var middlemanager_crosscluster_desired_count=1 \
  -var historical_desired_count=3 \
  -var query_desired_count=1 \
  -var query_cc_desired_count=1
