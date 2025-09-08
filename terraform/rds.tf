# VPC
resource "aws_vpc" "rds_vpc" {
  cidr_block = "10.0.0.0/16"

  tags = {
    Name = "rds_vpc"
  }
}

# Subnets
resource "aws_subnet" "private_a" {
  vpc_id                  = aws_vpc.rds_vpc.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "eu-west-2a"
}

resource "aws_subnet" "private_b" {
  vpc_id                  = aws_vpc.rds_vpc.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "eu-west-2b"
}

# Subnet group for RDS
resource "aws_db_subnet_group" "warehouse-subnet-group" {
  subnet_ids = [aws_subnet.private_a.id, 
                aws_subnet.private_b.id]

  tags = {
    Name = "warehouse_subnets"
  }
}

# Route tables group for the subnets
resource "aws_route_table" "private_a" {
  vpc_id = aws_vpc.rds_vpc.id
}

resource "aws_route_table_association" "private_a_assoc" {
  subnet_id      = aws_subnet.private_a.id
  route_table_id = aws_route_table.private_a.id
}

resource "aws_route_table" "private_b" {
  vpc_id = aws_vpc.rds_vpc.id
}

resource "aws_route_table_association" "private_b_assoc" {
  subnet_id      = aws_subnet.private_b.id
  route_table_id = aws_route_table.private_b.id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.rds_vpc.id
  service_name      = "com.amazonaws.eu-west-2.s3"
  vpc_endpoint_type = "Gateway"

  route_table_ids = [
  aws_route_table.private_a.id,
  aws_route_table.private_b.id
]
}

# Security group for RDS
resource "aws_security_group" "warehouse_sg" {
  name        = "warehouse-sg"
  description = "Security group for RDS"
  vpc_id      = aws_vpc.rds_vpc.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    security_groups = [aws_security_group.lambda_sg.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Security group for Lambda
resource "aws_security_group" "lambda_sg" {
name        = "lambda-sg"
  description = "Security group for Lambda"
  vpc_id = aws_vpc.rds_vpc.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# RDS instance
resource "aws_db_instance" "warehouse" {
  identifier             = "data-warehouse"
  engine                 = "postgres"
  instance_class         = "db.t3.micro"
  allocated_storage      = 25
  db_name                = "warehouse"

  username               = var.warehouse_username
  password               = var.warehouse_password

  vpc_security_group_ids = [aws_security_group.warehouse_sg.id]
  db_subnet_group_name   = aws_db_subnet_group.warehouse-subnet-group.name
  publicly_accessible    = false
  skip_final_snapshot    = true
}




