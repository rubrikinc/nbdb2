#! /bin/bash

# Exit when any command fails
set -e

echo "Starting Flask & Indexing consumer"
python3.7 deploy_aws.py --target=flask --state=up
python3.7 deploy_aws.py --target=indexing_consumer --state=up

echo "Starting prod metric consumer"
python3.7 deploy_aws.py --target=prod_metric_consumer --state=up --num_instances=1
