module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.5.2"

  name = "${local.project_name}-vpc"
  cidr = "10.0.0.0/16"

  # Updated Availability Zones for the eu-central-1 (Frankfurt) region.
  azs             = ["eu-central-1a", "eu-central-1b"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24"]

  # A NAT Gateway allows resources in private subnets (like EMR)
  # to access the internet for things like downloading packages,
  # without being directly exposed.
  enable_nat_gateway = true
  single_nat_gateway = true

  tags = local.common_tags
}