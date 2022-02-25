#! /bin/bash

# Exit when any command fails
set -e

echo "Starting Flask"
python3.7 deploy_aws.py --target=flask --state=up

echo "Starting test metric producer"
python3.7 deploy_aws.py --target=test_metric_producer --state=up

echo "Starting prod metric consumer"
python3.7 deploy_aws.py --target=prod_metric_consumer --state=up --num_instances=1
