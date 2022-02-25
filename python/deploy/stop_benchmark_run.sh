#! /bin/bash

# Exit when any command fails
set -e

echo "Stopping all services"
python3.7 deploy_aws.py --target=flask --state=down
python3.7 deploy_aws.py --target=test_metric_producer --state=down --start_cluster_id=0

python3.7 deploy_aws.py --target=test_metric_reader --state=down
python3.7 deploy_aws.py --target=metric_consumer --state=down
