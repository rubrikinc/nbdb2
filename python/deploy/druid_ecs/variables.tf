provider "aws" {
  region = "us-east-1"
}

variable "master_desired_count" {
  type = "string"
}

variable "zookeeper_desired_count" {
  type = "string"
}

variable "zookeeper_monitor_desired_count" {
  type = "string"
  default = "1"
}

variable "middlemanager_regular_desired_count" {
  type = "string"
}

variable "middlemanager_crosscluster_desired_count" {
  type = "string"
}

variable "historical_desired_count" {
  type = "string"
}

variable "query_desired_count" {
  type = "string"
}

variable "query_cc_desired_count" {
  type = "string"
}

variable "master_cluster" {
  type = "string"
}

variable "zookeeper_cluster" {
  type = "string"
}

variable "middlemanager_cluster" {
  type = "string"
}

variable "historical_cluster" {
  type = "string"
}

variable "query_cluster" {
  type = "string"
}

variable "zookeeper_tg_arn" {
  type = "string"
}

variable "coordinator_tg_arn" {
  type = "string"
}

variable "overlord_tg_arn" {
  type = "string"
}

variable "router_tg_arn" {
  type = "string"
}

variable "router_cc_tg_arn" {
  type = "string"
}


