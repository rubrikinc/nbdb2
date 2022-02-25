#!/bin/bash

# Add appropriate values for your kafka deployment in place of <ip-addr> and <zk-ip-port-x>
ssh ec2-user@<ip-addr> "~/kafka_2.12-2.2.1/bin/kafka-topics.sh --create --zookeeper <zk-ip-port-1,zk-ip-port-2,zk-ip-port-3> --partitions=50 --replication-factor 3 --topic anomaly_db_new_metrics"

ssh ec2-user@<ip-addr> "~/kafka_2.12-2.2.1bin/kafka-topics.sh --create --zookeeper <zk-ip-port-1,zk-ip-port-2,zk-ip-port-3> --partitions=50 --replication-factor 3 --topic test-master-line-metrics"

ssh ec2-user@<ip-addr> "~/kafka_2.12-2.2.1bin/kafka-topics.sh --create --zookeeper <zk-ip-port-1,zk-ip-port-2,zk-ip-port-3> --partitions=50 --replication-factor 3 --topic anomalydb_sparse_batch_consumer"

ssh ec2-user@<ip-addr> "~/kafka_2.12-2.2.1bin/kafka-topics.sh --create --zookeeper <zk-ip-port-1,zk-ip-port-2,zk-ip-port-3> --partitions=50 --replication-factor 3 --topic anomalydb_dense_batch_consumer"

echo "Topics created"
