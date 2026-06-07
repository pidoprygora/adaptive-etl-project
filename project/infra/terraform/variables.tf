variable "project_name" {
  description = "Project name prefix for AWS resources"
  type        = string
  default     = "k3d-airflow"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-north-1"
}

variable "instance_type" {
  description = "EC2 instance type (4 vCPU / 16 GB recommended for Airflow + Spark)"
  type        = string
  default     = "m7i.xlarge"
}

variable "root_volume_size_gb" {
  description = "Root EBS size in GiB"
  type        = number
  default     = 60
}

variable "allowed_cidr" {
  description = "CIDR blocks allowed to reach SSH (22) and Airflow UI (8080)"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "public_key_path" {
  description = "Path to local SSH public key registered as EC2 key pair"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "cluster_name" {
  description = "k3d cluster name to create on EC2"
  type        = string
  default     = "airflow"
}

variable "airflow_version" {
  description = "Apache Airflow Helm chart version"
  type        = string
  default     = "1.15.0"
}

variable "airflow_image_tag" {
  description = "Apache Airflow Docker image tag"
  type        = string
  default     = "2.10.4"
}

variable "executor" {
  description = "Airflow executor type"
  type        = string
  default     = "LocalExecutor"
}

variable "airflow_admin_password" {
  description = "Password for the Airflow webserver admin user"
  type        = string
  sensitive   = true
}

variable "s3_bucket" {
  description = "Existing S3 bucket name for Athena/Spark data (raw + processed)"
  type        = string
}

variable "athena_output_location" {
  description = "S3 URI for Athena query results (e.g. s3://bucket/athena-results/)"
  type        = string
}

variable "spark_version" {
  description = "Apache Spark version to install on EC2"
  type        = string
  default     = "3.5.1"
}

variable "cloudwatch_namespace" {
  description = "Namespace used by CloudWatch Agent for host metrics"
  type        = string
  default     = "AdaptiveETL/Host"
}

variable "cloudwatch_metrics_interval_sec" {
  description = "CloudWatch Agent metrics collection interval in seconds"
  type        = number
  default     = 60
}
