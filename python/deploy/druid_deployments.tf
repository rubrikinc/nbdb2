terraform {
  required_version = ">= 0.10.0"

  # EDIT - with your S3 cred
  # The S3 backend is used by Terraform to store the current state of the AWS
  # resources managed in this file
  backend "s3" {
    bucket = "your.s3-bucket.com"
    key = "private/your/terraform/druid/"
    region = "us-east-1"
  }
}

# AnomalyDB prod deployment
module "prod_druid_deployment" {
  source = "./druid_ecs"
  # ECS cluster info
  master_cluster = "anomalydb-druid-master"
  zookeeper_cluster = "anomalydb-druid-zookeeper"
  middlemanager_cluster = "anomalydb-druid-middlemanager"
  historical_cluster = "anomalydb-druid-historical"
  query_cluster = "anomalydb-druid-query"

  # EDIT - with your target group info
  zookeeper_tg_arn = "arn:aws:<id>:targetgroup/anomalydb-druid-master-zookeeper/abccbd1b574dda8e"
  coordinator_tg_arn = "arn:aws:<id>:targetgroup/anomalydb-druid-master-coordinat/abc1d3b918941c8f"
  overlord_tg_arn = "arn:aws:<id>:targetgroup/anomalydb-druid-master-overlord/abcee065ce6fcb5g"
  router_tg_arn = "arn:aws:<id>:targetgroup/anomalydb-router/abc25933b08ead1h"
  router_cc_tg_arn = "arn:aws:<id>:targetgroup/anomalydb-cc-router/abc2b3d7a354a19k"

  master_desired_count = "${var.master_desired_count}"
  zookeeper_desired_count = "${var.zookeeper_desired_count}"
  zookeeper_monitor_desired_count = "${var.zookeeper_monitor_desired_count}"
  middlemanager_regular_desired_count = "${var.middlemanager_regular_desired_count}"
  middlemanager_crosscluster_desired_count = "${var.middlemanager_crosscluster_desired_count}"
  historical_desired_count = "${var.historical_desired_count}"
  query_desired_count = "${var.query_desired_count}"
  query_cc_desired_count = "${var.query_cc_desired_count}"
}

# AnomalyDB Turbo deployment
module "turbo_druid_deployment" {
  source = "./druid_ecs"
  # ECS cluster info
  master_cluster = "turbo-anomalydb-druid-master"
  zookeeper_cluster = "turbo-anomalydb-druid-zookeeper"
  middlemanager_cluster = "turbo-anomalydb-druid-middlemanager"
  historical_cluster = "turbo-anomalydb-druid-historical"
  query_cluster = "turbo-anomalydb-druid-query"

  # EDIT - with your target group info
  zookeeper_tg_arn = "arn:aws:<id>:targetgroup/turbo-druid-master-zookeeper/abcf528063d6d121"
  coordinator_tg_arn = "arn:aws:<id>:targetgroup/turbo-druid-master-coordinator/abc06e1f0a351002"
  overlord_tg_arn = "arn:aws:<id>:targetgroup/turbo-druid-master-overlord/abc5332b45d96d53"
  router_tg_arn = ""
  router_cc_tg_arn = ""

  master_desired_count = "${var.master_desired_count}"
  zookeeper_desired_count = "${var.zookeeper_desired_count}"
  zookeeper_monitor_desired_count = "${var.zookeeper_monitor_desired_count}"
  middlemanager_regular_desired_count = "${var.middlemanager_regular_desired_count}"
  middlemanager_crosscluster_desired_count = "${var.middlemanager_crosscluster_desired_count}"
  historical_desired_count = "${var.historical_desired_count}"
  query_desired_count = "${var.query_desired_count}"
  query_cc_desired_count = "${var.query_cc_desired_count}"
}
