output "s3_bucket_name" {
  description = "The name of the S3 bucket for the data lake."
  value       = aws_s3_bucket.data_lake.bucket
}

output "kinesis_stream_name" {
  description = "The name of the Kinesis Data Stream."
  value       = aws_kinesis_stream.wikimedia_stream.name
}

output "iceberg_db_name" {
  description = "The name of the AWS Glue Catalog database for Iceberg."
  value       = aws_glue_catalog_database.iceberg_db.name
}

output "aws_region" {
  description = "The AWS region the infrastructure is deployed in."
  value       = data.aws_region.current.name
}