# ---------------------------
# EC2 Bastion for SSM
# ---------------------------

resource "aws_instance" "ssm_bastion" {
  ami           = "ami-09a2a0f7d2db8baca" # Amazon Linux 2 in eu-west-2 (London). Check for latest in your region
  instance_type = "t2.micro"

  subnet_id              = aws_subnet.private_a.id # <-- replace with one of your RDS subnets
  vpc_security_group_ids = [aws_security_group.bastion_sg.id]

  #key_name      = "bastion-key"

  iam_instance_profile = aws_iam_instance_profile.ssm_profile.name
  
  tags = {
    Name = "ssm-bastion"
  }
}

# ---------------------------
# Security Group for Bastion
# ---------------------------
resource "aws_security_group" "bastion_sg" {
  name        = "ssm-bastion-sg"
  description = "Allow bastion to reach RDS"
  vpc_id      = aws_vpc.rds_vpc.id # <-- your RDS VPC ID

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ---------------------------
# IAM Role for SSM
# ---------------------------
resource "aws_iam_role" "ssm_role" {
  name = "ssm-bastion-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm_attach" {
  role       = aws_iam_role.ssm_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ssm_profile" {
  name = "ssm-bastion-profile"
  role = aws_iam_role.ssm_role.name
}
