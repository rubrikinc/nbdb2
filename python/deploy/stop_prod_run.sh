#! /bin/bash

# Exit when any command fails
set -e

echo "Stopping all services"
python3.7 deploy_aws.py --target=flask --state=down
python3.7 deploy_aws.py --target=indexing_consumer --state=down
python3.7 deploy_aws.py --target=prod_metric_consumer --state=down
