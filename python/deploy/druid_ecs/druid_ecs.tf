##########################
# Druid Zookeeper ensemble
##########################

# 1. Druid zookeeper ECS service
resource "aws_ecs_task_definition" "druid_master_zookeeper" {
  family                = "anomalydb-druid-zookeeper"
  container_definitions = "${file("compose/druid_zookeeper.json")}"
  network_mode          = "host"
}

resource "aws_ecs_service" "druid_master_zookeeper" {
  name            = "anomalydb-druid-zookeeper"
  cluster         = "${var.zookeeper_cluster}"
  task_definition = "${aws_ecs_task_definition.druid_master_zookeeper.arn}"
  desired_count   = "${var.zookeeper_desired_count}"

  ordered_placement_strategy {
    type  = "binpack"
    field = "memory"
  }
  
  load_balancer {
    target_group_arn = "${var.zookeeper_tg_arn}"
    container_name   = "druid_zookeeper"
    container_port   = 2181
  }
}

# 2. Druid zookeeper monitor ECS service

resource "aws_ecs_task_definition" "druid_master_zookeeper_monitor" {
  family                = "anomalydb-druid-zookeeper-monitor"
  container_definitions = "${file("compose/druid_zookeeper_monitor.json")}"
  network_mode          = "host"
}

resource "aws_ecs_service" "druid_master_zookeeper_monitor" {
  name            = "anomalydb-druid-zookeeper-monitor"
  cluster         = "${var.zookeeper_cluster}"
  task_definition = "${aws_ecs_task_definition.druid_master_zookeeper_monitor.arn}"
  desired_count   = "${var.zookeeper_monitor_desired_count}"

}
#########################
# Druid master nodes
#########################

# Master node consists of 2 ECS services:
# 1. Coordinator
# 2. Overlord

# 1. Druid master coordinator task definition
resource "aws_ecs_task_definition" "druid_master_coordinator" {
  family                = "anomalydb-druid-coordinator"
  container_definitions = "${file("compose/druid_coordinator.json")}"
  network_mode          = "host"

  # Need to mount /dev for getting disk metrics to work
  volume {
    name      = "volume-0"
    host_path = "/dev"
  }
}

# 1. Druid master coordinator ECS service
resource "aws_ecs_service" "druid_master_coordinator" {
  name            = "anomalydb-druid-coordinator"
  cluster         = "${var.master_cluster}"
  task_definition = "${aws_ecs_task_definition.druid_master_coordinator.arn}"
  desired_count   = "${var.master_desired_count}"

  ordered_placement_strategy {
    type  = "binpack"
    field = "memory"
  }

  load_balancer {
    target_group_arn = "${var.coordinator_tg_arn}"
    container_name   = "druid_coordinator"
    container_port   = 8081
  }

  # Coordinator does a lot of work trying to recreate lost state esp if other
  # Druid components did not restart. Coordinator is likely to fail health
  # checks during this period. Give it a grace period of 10m at the beginning
  health_check_grace_period_seconds = 600
}

# 2. Druid master overlord task definition
resource "aws_ecs_task_definition" "druid_master_overlord" {
  family                = "anomalydb-druid-overlord"
  container_definitions = "${file("compose/druid_overlord.json")}"
  network_mode          = "host"

  # Need to mount /dev for getting disk metrics to work
  volume {
    name      = "volume-0"
    host_path = "/dev"
  }
}

# 2. Druid master overlord ECS service
resource "aws_ecs_service" "druid_master_overlord" {
  name            = "anomalydb-druid-overlord"
  cluster         = "${var.master_cluster}"
  task_definition = "${aws_ecs_task_definition.druid_master_overlord.arn}"
  desired_count   = "${var.master_desired_count}"

  ordered_placement_strategy {
    type  = "binpack"
    field = "memory"
  }

  load_balancer {
    target_group_arn = "${var.overlord_tg_arn}"
    container_name   = "druid_overlord"
    container_port   = 8090
  }

  # Overlord does a lot of work trying to recreate lost state esp if other
  # Druid components did not restart. Overlord is likely to fail health checks
  # during this period. Give it a grace period of 10m at the beginning
  health_check_grace_period_seconds = 600
}

#########################
# Druid middlemanager
#########################

# Druid regular MM task definition
resource "aws_ecs_task_definition" "druid_mm_regular" {
  family                = "anomalydb-druid-mm-regular"
  container_definitions = "${file("compose/druid_middlemanager.json")}"
  network_mode          = "host"

  # Need to mount /dev for getting disk metrics to work
  volume {
    name      = "volume-0"
    host_path = "/dev"
  }
}

# Druid regular MM ECS service
resource "aws_ecs_service" "druid_mm_regular" {
  name            = "anomalydb-druid-mm-regular"
  cluster         = "${var.middlemanager_cluster}"
  task_definition = "${aws_ecs_task_definition.druid_mm_regular.arn}"
  desired_count   = "${var.middlemanager_regular_desired_count}"

  ordered_placement_strategy {
    type  = "binpack"
    field = "memory"
  }
}

# Druid crosscluster MM task definition
resource "aws_ecs_task_definition" "druid_mm_crosscluster" {
  family                = "anomalydb-druid-mm-crosscluster"
  container_definitions = "${file("compose/druid_middlemanager.json")}"
  network_mode          = "host"

  # Need to mount /dev for getting disk metrics to work
  volume {
    name      = "volume-0"
    host_path = "/dev"
  }
}

# Druid crosscluster MM ECS service
resource "aws_ecs_service" "druid_mm_crosscluster" {
  name            = "anomalydb-druid-mm-crosscluster"
  cluster         = "${var.middlemanager_cluster}"
  task_definition = "${aws_ecs_task_definition.druid_mm_crosscluster.arn}"
  desired_count   = "${var.middlemanager_crosscluster_desired_count}"

  ordered_placement_strategy {
    type  = "binpack"
    field = "memory"
  }
}


#########################
# Druid historical nodes
#########################

# Druid historical node task definition
resource "aws_ecs_task_definition" "druid_historical" {
  family                = "anomalydb-druid-historical"
  container_definitions = "${file("compose/druid_historical.json")}"
  network_mode          = "host"

  # Historical node containers mount the cold HDD EBS as a Docker volume
  volume {
    name      = "volume-0"
    host_path = "/mnt-hdd"
  }
  # Need to mount /dev for getting disk metrics to work
  volume {
    name      = "volume-1"
    host_path = "/dev"
  }
}

# Druid historical ECS service
resource "aws_ecs_service" "druid_historical" {
  name            = "anomalydb-druid-historical"
  cluster         = "${var.historical_cluster}"
  task_definition = "${aws_ecs_task_definition.druid_historical.arn}"
  desired_count   = "${var.historical_desired_count}"

  ordered_placement_strategy {
    type  = "binpack"
    field = "cpu"
  }
}

#########################
# Druid query nodes
#########################

# Druid query node task definition
resource "aws_ecs_task_definition" "druid_query" {
  family                = "anomalydb-druid-query"
  container_definitions = "${file("compose/druid_query.json")}"
  network_mode          = "host"

  # Need to mount /dev for getting disk metrics to work
  volume {
    name      = "volume-0"
    host_path = "/dev"
  }
}

# Druid query nodes ECS service
resource "aws_ecs_service" "druid_query" {
  name            = "anomalydb-druid-query"
  cluster         = "${var.query_cluster}"
  task_definition = "${aws_ecs_task_definition.druid_query.arn}"
  desired_count   = "${var.query_desired_count}"

  ordered_placement_strategy {
    type  = "binpack"
    field = "cpu"
  }

  load_balancer {
    target_group_arn = "${var.router_tg_arn}"
    container_name   = "druid_query"
    container_port   = 80
  }
}

# Druid query crosscluster node task definition
resource "aws_ecs_task_definition" "druid_query_cc" {
  family                = "anomalydb-druid-query_cc"
  container_definitions = "${file("compose/druid_query.json")}"
  network_mode          = "host"

  # Need to mount /dev for getting disk metrics to work
  volume {
    name      = "volume-0"
    host_path = "/dev"
  }
}

# Druid query crosscluster nodes ECS service
resource "aws_ecs_service" "druid_query_cc" {
  name            = "anomalydb-druid-query_cc"
  cluster         = "${var.query_cluster}"
  task_definition = "${aws_ecs_task_definition.druid_query_cc.arn}"
  desired_count   = "${var.query_cc_desired_count}"

  ordered_placement_strategy {
    type  = "binpack"
    field = "cpu"
  }

  load_balancer {
    target_group_arn = "${var.router_cc_tg_arn}"
    container_name   = "druid_query"
    container_port   = 80
  }
}



