locals {
  default_subnet_id = element(sort(data.aws_subnets.default.ids), 0)
}
