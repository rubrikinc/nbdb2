#!/bin/bash -eux

echo "Deleting Kafka topics"
# TODO: Deleting kafka topics
#ssh ec2-user@<ip-addr> "/home/ec2-user/kafka_2.12-2.2.1/bin/kafka-topics.sh --delete --zookeeper  <zk-ip-port-1,zk-ip-port-2,zk-ip-port-3> --topic test-master-line-metrics"

#exit 0

echo "Starting Druid deployment"
python3.7 deploy_aws.py --target=druid_deployment --mode=prod --state=up \
  --master_desired_count=1 --zookeeper_desired_count=3 \
  --middlemanager_regular_desired_count=38 \
  --middlemanager_crosscluster_desired_count=20 \
  --historical_desired_count=20 \
  --query_desired_count=8 \
  --query_cc_desired_count=8

echo "Waiting for 2 min test give Druid deployment time to settle"
echo "-----------------------------------------------------------"
for t in {1..2}; do echo "Sleeping $t/2 mins" && sleep 60; done

#echo "Starting test metric producer_0"
#python3.7 deploy_aws.py --target=test_metric_producer --state=up --start_cluster_id=0 --setup=1
#echo "Waiting for 1 min test give TMP time to create the schema"
#echo "-----------------------------------------------------------"
#for t in {1..6}; do echo "Sleeping ${t}0/60 seconds" && sleep 10; done


#echo "Waiting for 2 mins before starting metric consumer"
#echo "-----------------------------------------------------------"

#for t in {1..2}; do echo "Sleeping $t/2 mins" && sleep 60; done


echo "Starting Read API instances"
python3.7 deploy_aws.py --target=read_api --state=up --num_instances=8

echo "Starting Influx & Graphite metric consumers"
# num_instances os the instances are subject to tuning 
python3.7 deploy_aws.py --target=master_graphite_consumer --state=up \
	--num_instances=2 --consumermode=realtime
python3.7 deploy_aws.py --target=prod_no_agg_graphite_consumer --state=up \
	--num_instances=50 --consumermode=realtime
python3.7 deploy_aws.py --target=prod_graphite_consumer --state=up \
	--num_instances=1 --consumermode=realtime
python3.7 deploy_aws.py --target=v4_live_influx_consumer --state=up \
	--num_instances=2 --consumermode=realtime
python3.7 deploy_aws.py --target=internal_graphite_consumer --state=up \
	--num_instances=1 --consumermode=realtime

# num_instances os the instances are subject to tuning
python3.7 deploy_aws.py --target=master_graphite_consumer --state=up \
	--num_instances=2 --consumermode=rollup
python3.7 deploy_aws.py --target=prod_no_agg_graphite_consumer --state=up \
	--num_instances=50 --consumermode=rollup
python3.7 deploy_aws.py --target=prod_graphite_consumer --state=up \
	--num_instances=1 --consumermode=rollup
python3.7 deploy_aws.py --target=v4_live_influx_consumer --state=up \
	--num_instances=2 --consumermode=rollup
python3.7 deploy_aws.py --target=internal_graphite_consumer --state=up \
	--num_instances=1 --consumermode=rollup

echo "Starting batch consumers"
python3.7 deploy_aws.py --target=sparse_batch_consumer --state=up \
	--num_instances=1
python3.7 deploy_aws.py --target=dense_batch_consumer --state=up \
	--num_instances=8
