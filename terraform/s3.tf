# This resource is needed to generate a unique suffix for the bucket name
resource "random_id" "bucket_suffix" {
  byte_length = 8
}

resource "aws_s3_bucket" "data_lake" {
  # The bucket name includes a random suffix to ensure it's globally unique
  bucket = "${local.project_name}-data-lake-${random_id.bucket_suffix.hex}"

  # --- THIS IS THE FIX ---
  # This tells Terraform to delete all objects in the bucket before destroying the bucket itself.
  # This is safe for our ephemeral data lake but should be used with caution in production.
  force_destroy = true
}

# Enable versioning as a best practice, which also helps with data lifecycle management.
resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Block all public access to the bucket for security.
resource "aws_s3_bucket_public_access_block" "data_lake_access_block" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}