# This creates a private and secure route from our VPC to the DynamoDB service.
# It's essential for the EMR job in the private subnet to create its checkpoint table.
resource "aws_vpc_endpoint" "dynamodb_endpoint" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.dynamodb"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.private_route_table_ids
  tags              = local.common_tags
}

# This creates a private and secure route from our VPC to the S3 service.
# It's essential for the EMR service to download the job scripts (e.g., streaming_job.py)
# from S3 before the job can even start. This is a common cause of jobs being
# stuck in the 'Running' state without making progress.
resource "aws_vpc_endpoint" "s3_endpoint" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.private_route_table_ids
  tags              = local.common_tags
}

# This creates a private and secure route from our VPC to the Kinesis service.
# It's essential for the Spark application, once running, to pull data from the
# Kinesis stream. Without this, the job would start but fail to read any records.
resource "aws_vpc_endpoint" "kinesis_endpoint" {
  vpc_id              = module.vpc.vpc_id
  service_name        = "com.amazonaws.${data.aws_region.current.name}.kinesis-streams"
  vpc_endpoint_type   = "Interface" # Kinesis uses an Interface endpoint
  subnet_ids          = module.vpc.private_subnets
  security_group_ids  = [aws_security_group.emr_sg.id]
  private_dns_enabled = true
  tags                = local.common_tags
}