resource "aws_key_pair" "this" {
  key_name   = "${var.project_name}-key"
  public_key = file(var.public_key_path)
}

resource "aws_security_group" "ec2_sg" {
  name        = "${var.project_name}-sg"
  description = "SSH and Airflow UI access"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr
  }

  ingress {
    description = "Airflow UI"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr
  }

  egress {
    description = "Outbound internet"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-sg"
  }
}

resource "aws_instance" "this" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.this.key_name
  subnet_id              = local.default_subnet_id
  vpc_security_group_ids = [aws_security_group.ec2_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name

  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 8
  }

  root_block_device {
    volume_size           = var.root_volume_size_gb
    volume_type           = "gp3"
    delete_on_termination = true
  }

  user_data_base64 = base64gzip(templatefile("${path.module}/templates/user_data.sh.tftpl", {
    cluster_name           = var.cluster_name
    airflow_version        = var.airflow_version
    airflow_image_tag      = var.airflow_image_tag
    executor               = var.executor
    airflow_admin_password = var.airflow_admin_password
    aws_region             = var.aws_region
    s3_bucket              = var.s3_bucket
    athena_output_location = var.athena_output_location
    spark_version          = var.spark_version
    cloudwatch_namespace   = var.cloudwatch_namespace
    cloudwatch_interval    = var.cloudwatch_metrics_interval_sec
    cloudwatch_agent_config = templatefile("${path.module}/templates/amazon-cloudwatch-agent.json.tftpl", {
      cloudwatch_namespace = var.cloudwatch_namespace
      cloudwatch_interval  = var.cloudwatch_metrics_interval_sec
    })
  }))

  tags = {
    Name    = "${var.project_name}-ec2"
    Purpose = "k3d-airflow"
  }
}
