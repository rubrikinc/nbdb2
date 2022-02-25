#! /bin/bash

# Exit when any command fails
set -e
echo "Deleting Kafka topics"
# TODO: Delete kafka topics
#ssh ec2-user@<ip-addr> "/home/ec2-user/kafka_2.12-2.2.1/bin/kafka-topics.sh --delete --zookeeper <zk-ip-port-1,zk-ip-port-2,zk-ip-port-3> --topic test-master-line-metrics"

echo "Starting test metric producer_0"
python3.7 deploy_aws.py --target=test_metric_producer --state=up --start_cluster_id=0 --setup=1
echo "Waiting for 1 min test give TMP time to create the schema"
echo "-----------------------------------------------------------"
#for t in {1..6}; do echo "Sleeping $t/60 seconds" && sleep 10; done

#echo "Starting remaining TMPs"
#python3.7 deploy_aws.py --target=test_metric_producer --state=up --start_cluster_id=400 --setup=0 &


echo "Waiting for 2 mins before starting metric consumer"
echo "-----------------------------------------------------------"

for t in {1..2}; do echo "Sleeping $t/2 mins" && sleep 60; done


echo "Starting metric consumer"
python3.7 deploy_aws.py --target=metric_consumer --state=up --num_instances=1


echo "Starting Flask"
python3.7 deploy_aws.py --target=flask --state=up

echo "Starting test metric reader"
#python3.7 deploy_aws.py --target=test_metric_reader --state=up


