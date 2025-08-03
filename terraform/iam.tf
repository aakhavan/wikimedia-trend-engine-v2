# --- IAM Role for EMR Serverless ---
resource "aws_iam_role" "emr_serverless_role" {
  name = "${local.project_name}-emr-serverless-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = "emr-serverless.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_policy" "emr_serverless_policy" {
  name   = "${local.project_name}-emr-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ],
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "glue:GetDatabase",
          "glue:CreateDatabase",
          "glue:GetDataBases",
          "glue:CreateTable",
          "glue:GetTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetTables",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchDeletePartition"
        ],
        Resource = [
          "arn:aws:glue:${data.aws_partition.current.id}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_partition.current.id}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.iceberg_db.name}",
          "arn:aws:glue:${data.aws_partition.current.id}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.iceberg_db.name}/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "dynamodb:CreateTable",
          "dynamodb:DescribeTable",
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem"
        ],
        Resource = "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/WikimediaStreamingDStream"
      },
      {
        # --- THIS IS THE FINAL FIX ---
        # Permissions for the EMR role to create and manage Elastic Network Interfaces (ENIs).
        # This is required for the job to communicate with services via an Interface VPC Endpoint (like Kinesis).
        # Without this, the connection to Kinesis will time out, as seen in the logs.
        "Effect": "Allow",
        "Action": [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeSubnets"
        ],
        "Resource": "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_s3_glue_attachment" {
  role       = aws_iam_role.emr_serverless_role.name
  policy_arn = aws_iam_policy.emr_serverless_policy.arn
}

# Attach the AWS managed policy for Kinesis read access.
resource "aws_iam_role_policy_attachment" "emr_kinesis_attachment" {
  role       = aws_iam_role.emr_serverless_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonKinesisReadOnlyAccess"
}


# --- IAM Role for Airflow EC2 Instance ---
resource "aws_iam_role" "airflow_ec2_role" {
  name = "${local.project_name}-airflow-ec2-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = "ec2.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_policy" "airflow_ec2_policy" {
  name   = "${local.project_name}-airflow-ec2-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        # Permissions to start, get, and cancel EMR Serverless jobs.
        Effect = "Allow",
        Action = [
          "emr-serverless:StartJobRun",
          "emr-serverless:GetJobRun",
          "emr-serverless:CancelJobRun"
        ],
        Resource = aws_emrserverless_application.spark_app.arn
      },
      {
        # Permission for the EC2 instance to pass the EMR role to the EMR service.
        Effect   = "Allow",
        Action   = "iam:PassRole",
        Resource = aws_iam_role.emr_serverless_role.arn
      },
      {
        # Permissions to upload Spark scripts to S3.
        Effect = "Allow",
        Action = [
            "s3:ListBucket",
            "s3:PutObject"
        ],
        Resource = [
            aws_s3_bucket.data_lake.arn,
            "${aws_s3_bucket.data_lake.arn}/spark_scripts/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "airflow_ec2_attachment" {
  role       = aws_iam_role.airflow_ec2_role.name
  policy_arn = aws_iam_policy.airflow_ec2_policy.arn
}

resource "aws_iam_instance_profile" "airflow_instance_profile" {
  name = "${local.project_name}-airflow-instance-profile"
  role = aws_iam_role.airflow_ec2_role.name
}


data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}
data "aws_region" "current" {} # Added to support the DynamoDB ARN