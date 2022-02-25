#!/bin/bash -eux

echo "Rebuilding AnomalyDB docker images"
python3.7 deploy_aws.py --target=druid_query --mode=prod --state=build
python3.7 deploy_aws.py --target=druid_query_cc --mode=prod --state=build

# We do not stop query nodes to avoid any downtime for read queries. Instead we
# just redeploy with a new task definition version. The old version will slowly
# be replaced by the new version. This requires spare EC2 instances though
echo "Starting Druid query node"
terraform apply -auto-approve \
  -target=module.prod_druid_deployment.aws_ecs_service.druid_query \
  -var master_desired_count=1 \
  -var zookeeper_desired_count=3 \
  -var middlemanager_regular_desired_count=1 \
  -var middlemanager_crosscluster_desired_count=1 \
  -var historical_desired_count=1 \
  -var query_desired_count=24 \
  -var query_cc_desired_count=24

# We do not stop query nodes to avoid any downtime for read queries. Instead we
# just redeploy with a new task definition version. The old version will slowly
# be replaced by the new version. This requires spare EC2 instances though
echo "Starting Druid CC query node"
terraform apply -auto-approve \
  -target=module.prod_druid_deployment.aws_ecs_service.druid_query_cc \
  -var master_desired_count=1 \
  -var zookeeper_desired_count=3 \
  -var middlemanager_regular_desired_count=1 \
  -var middlemanager_crosscluster_desired_count=1 \
  -var historical_desired_count=1 \
  -var query_desired_count=24 \
  -var query_cc_desired_count=24
