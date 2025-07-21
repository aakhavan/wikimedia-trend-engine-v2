resource "aws_emrserverless_application" "spark_app" {
  name          = "${local.project_name}-spark-app"
  release_label = "emr-6.15.0" # A recent and stable Spark 3 version (Spark 3.4.1)
  type          = "SPARK"

  # Define a small initial capacity for the driver to start quickly.
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

  maximum_capacity {
    cpu    = "8vCPU"
    memory = "32GB"
  }

  tags = local.common_tags
}