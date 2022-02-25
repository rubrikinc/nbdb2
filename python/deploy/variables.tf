provider "aws" {
  region = "us-east-1"
  version = "v2.70.0"
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

