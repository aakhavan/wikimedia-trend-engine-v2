output "data_lake_s3_bucket_name" {
  description = "The name of the S3 bucket for the data lake."
  value       = aws_s3_bucket.data_lake.bucket
}

output "kinesis_stream_name" {
  description = "The name of the Kinesis Data Stream."
  value       = aws_kinesis_stream.wikimedia_stream.name
}

output "emr_serverless_app_id" {
  description = "The ID of the EMR Serverless application."
  value       = aws_emrserverless_application.spark_app.id
}

output "emr_execution_role_arn" {
  description = "The ARN of the IAM role for EMR Serverless jobs."
  value       = aws_iam_role.emr_serverless_role.arn
}

output "airflow_public_ip" {
  description = "The public IP address of the EC2 instance hosting Airflow."
  value       = aws_instance.airflow_host.public_ip
}