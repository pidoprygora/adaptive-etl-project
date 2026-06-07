terraform {
  required_version = "~> 1.8"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Uncomment to use remote S3 backend:
  # backend "s3" {
  #   bucket = "your-tfstate-bucket"
  #   key    = "k3d-airflow/terraform.tfstate"
  #   region = "us-east-1"
  # }
}
