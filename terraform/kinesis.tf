resource "aws_kinesis_stream" "wikimedia_stream" {
  name        = "${local.project_name}-stream"
  shard_count = 1 # Sufficient for this project and stays in Free Tier

  tags = local.common_tags
}