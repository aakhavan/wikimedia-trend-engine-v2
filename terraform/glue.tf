resource "aws_glue_catalog_database" "iceberg_db" {
  name = "${local.project_name}-db"
}