# A dedicated security group for the EMR Serverless application.
# It allows all outbound traffic so Spark can download packages from Maven.
resource "aws_security_group" "emr_sg" {
  name        = "${local.project_name}-emr-sg"
  description = "Allow all outbound traffic for EMR Serverless"
  vpc_id      = module.vpc.vpc_id

  # Allow ingress traffic from any resource within this same security group.
  # This is critical for the EMR job to communicate with the Kinesis Interface Endpoint,
  # as both are associated with this security group. Without this, the connection times out.
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1" # Allow all protocols
    self      = true # The source is this security group itself
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}


resource "aws_emrserverless_application" "spark_app" {
  name          = "${local.project_name}-spark-app"
  release_label = "emr-6.15.0" # Spark 3.4.1
  type          = "SPARK"

  initial_capacity {
    initial_capacity_type = "Driver"
    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "2vCPU"
        memory = "4GB"
      }
    }
  }

  # The maximum resources the application can scale up to.
  # This was increased to provide enough capacity for the driver and executors,
  # based on the ApplicationMaxCapacityExceededException in the logs.
  maximum_capacity {
    cpu    = "8vCPU"
    memory = "32GB"
  }


  network_configuration {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [aws_security_group.emr_sg.id]
  }

  tags = local.common_tags
}