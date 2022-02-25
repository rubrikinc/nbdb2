#!/bin/bash
kafka-topics --zookeeper localhost:2181 --delete -topic metrics
