output "instance_id" {
  description = "EC2 instance ID"
  value       = aws_instance.this.id
}

output "instance_public_ip" {
  description = "Public IP of EC2 instance"
  value       = aws_instance.this.public_ip
}

output "airflow_url" {
  description = "Airflow webserver URL"
  value       = "http://${aws_instance.this.public_ip}:8080"
}

output "ssh_command" {
  description = "SSH command to connect to EC2"
  value       = "ssh -i ~/.ssh/id_rsa ec2-user@${aws_instance.this.public_ip}"
}

output "ssm_command" {
  description = "AWS SSM session command (no SSH key required)"
  value       = "aws ssm start-session --target ${aws_instance.this.id}"
}

output "vpc_id" {
  description = "VPC ID used by the instance"
  value       = data.aws_vpc.default.id
}

output "aws_region" {
  description = "AWS region"
  value       = var.aws_region
}

output "athena_output_location" {
  description = "S3 URI for Athena query results"
  value       = var.athena_output_location
}
