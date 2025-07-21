data "aws_ami" "amazon_linux_2" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }
}

resource "aws_security_group" "airflow_sg" {
  name        = "${local.project_name}-airflow-sg"
  description = "Allow SSH and Airflow UI access"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description = "Allow SSH from my IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ip]
  }

  ingress {
    description = "Allow Airflow UI from my IP"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ip]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}

resource "aws_instance" "airflow_host" {
  ami                    = data.aws_ami.amazon_linux_2.id
  instance_type          = "t2.micro" # Free Tier eligible
  subnet_id              = module.vpc.public_subnets[0]
  vpc_security_group_ids = [aws_security_group.airflow_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.airflow_instance_profile.name
  associate_public_ip_address = true

  # This user_data script runs on instance startup.
  # It installs Docker, clones your repo, and starts Airflow.
  user_data = <<-EOF
              #!/bin/bash
              sudo yum update -y
              sudo yum install -y docker git
              sudo service docker start
              sudo usermod -a -G docker ec2-user

              # Install Docker Compose
              sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
              sudo chmod +x /usr/local/bin/docker-compose

              # Clone repo and start Airflow
              cd /home/ec2-user
              # --- THIS LINE IS CORRECTED ---
              git clone https://github.com/aakhavan/wikimedia-trend-engine-v2.git
              cd wikimedia-trend-engine-v2

              # Set Airflow UID to not have permissions issues with mounted volumes
              echo -e "AIRFLOW_UID=$(id -u ec2-user)\nAIRFLOW_GID=0" > .env

              # Start Airflow in detached mode
              /usr/local/bin/docker-compose up -d
              EOF

  tags = merge(
    local.common_tags,
    {
      Name = "${local.project_name}-airflow-host"
    }
  )
}