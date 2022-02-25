#!/bin/bash
trap 'kill $(jobs -p)' EXIT

bash -c "./tail_container.sh anomalydb_metric_consumer_1 '0;31'"&
bash -c "./tail_container.sh anomalydb_test_metric_producer_1 '0;32'"&
bash -c "./tail_container.sh anomalydb_flask_1 '0;36'"&
bash -c "./tail_container.sh anomalydb_redis_1 '0;37'"&
# bash -c "./tail_druid.sh '0;33'"&
# bash -c "./tail_container.sh anomalydb_test_metric_reader_1 '0;33'"&
# bash -c "./tail_container.sh anomalydb_elasticsearch_1 '0;34'"&
# bash -c "./tail_container.sh anomalydb_scylladb_1 '0;35'"&
# bash -c "./tail_container.sh anomalydb_druid_1 '0;38'"&
# bash -c "./tail_container.sh anomalydb_kafka_1 '0;39'"&
wait
