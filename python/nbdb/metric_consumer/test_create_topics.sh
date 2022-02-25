#!/bin/bash
kafka-topics --zookeeper localhost:2181 --create --replication-factor 1 --partitions 10 --topic metrics
