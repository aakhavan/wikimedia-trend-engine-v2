terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  # N. Virginia is cost-effective and has all services available.
  # It's a great default for most projects.
  region = "eu-central-1"
}

locals {
  project_name = "wikimedia-trends-v2"
  common_tags = {
    Project   = local.project_name
    ManagedBy = "Terraform"
  }
}