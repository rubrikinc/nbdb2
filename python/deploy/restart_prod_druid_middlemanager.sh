#!/bin/bash -eux

echo "Rebuilding AnomalyDB regular MM docker images"
python3.7 deploy_aws.py --target=druid_middlemanager_regular \
       	--mode=prod --state=build

echo "Stopping old regular MM nodes"
terraform apply -auto-approve \
  -target=module.prod_druid_deployment.aws_ecs_service.druid_mm_regular \
  -var master_desired_count=1 \
  -var zookeeper_desired_count=3 \
  -var middlemanager_regular_desired_count=0 \
  -var middlemanager_crosscluster_desired_count=20 \
  -var historical_desired_count=3 \
  -var query_desired_count=1 \
  -var query_cc_desired_count=1

echo "Waiting for 1 mins before starting regular MM node"
echo "-----------------------------------------------------------"
for t in {1..6}; do echo "Sleeping ${t}0/60 seconds" && sleep 10; done

echo "Starting new regular MM nodes"
terraform apply -auto-approve \
  -target=module.prod_druid_deployment.aws_ecs_service.druid_mm_regular \
  -var master_desired_count=1 \
  -var zookeeper_desired_count=3 \
  -var middlemanager_regular_desired_count=38 \
  -var middlemanager_crosscluster_desired_count=20 \
  -var historical_desired_count=3 \
  -var query_desired_count=1 \
  -var query_cc_desired_count=1

echo "Rebuilding AnomalyDB crosscluster MM docker images"
python3.7 deploy_aws.py --target=druid_middlemanager_crosscluster \
	--mode=prod --state=build

echo "Stopping old crosscluster MM nodes"
terraform apply -auto-approve \
  -target=module.prod_druid_deployment.aws_ecs_service.druid_mm_crosscluster \
  -var master_desired_count=1 \
  -var zookeeper_desired_count=3 \
  -var middlemanager_regular_desired_count=50 \
  -var middlemanager_crosscluster_desired_count=0 \
  -var historical_desired_count=3 \
  -var query_desired_count=1 \
  -var query_cc_desired_count=1

echo "Waiting for 1 mins before starting regular MM node"
echo "-----------------------------------------------------------"
for t in {1..6}; do echo "Sleeping ${t}0/60 seconds" && sleep 10; done

echo "Starting new crosscluster MM nodes"
terraform apply -auto-approve \
  -target=module.prod_druid_deployment.aws_ecs_service.druid_mm_crosscluster \
  -var master_desired_count=1 \
  -var zookeeper_desired_count=3 \
  -var middlemanager_regular_desired_count=50 \
  -var middlemanager_crosscluster_desired_count=20 \
  -var historical_desired_count=3 \
  -var query_desired_count=1 \
  -var query_cc_desired_count=1
