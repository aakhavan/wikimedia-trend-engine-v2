resource "aws_s3_bucket" "data_lake" {
  # The random_id ensures the bucket name is globally unique.
  bucket = "${local.project_name}-data-lake-${random_id.bucket_id.hex}"

  tags = local.common_tags
}

resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Helper to generate a unique suffix for the S3 bucket name.
resource "random_id" "bucket_id" {
  byte_length = 8
}