terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.9.0"
    }
  }
}

provider "aws" {
  region  = "us-east-1"
  default_tags {
    tags = {
      Environment = "Dev"
      Name = "ETL-Cenipa"
    }
  }
}

resource "aws_db_instance" "dwinstance" {
  allocated_storage    = 10
  db_name              = "dw"
  identifier           = "dwinstance" 
  engine               = "mysql"
  engine_version       = "8.0" 
  instance_class       = "db.t3.micro"
  username             = "root"
  password             = "rootrds23"
  parameter_group_name = "default.mysql8.0"
  skip_final_snapshot  = true
  publicly_accessible  = true
}

resource "aws_glue_connection" "rds_mysql_conn" {
  name = "rds_mysql"
  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:mysql:/${aws_db_instance.dwinstance.endpoint}/dw"
    USERNAME            = "root"
    PASSWORD            = "rootrds23"
  }
}

resource "aws_s3_bucket" "cenipa-bucket" {
  bucket        = "cenipa.etl.com.br"
  force_destroy = true 
}

resource "aws_s3_object" "object" {
  for_each = fileset("../data/", "**")
  bucket = aws_s3_bucket.cenipa-bucket.id
  key = "source/${each.value}"
  source = "../data/${each.value}"
  etag = filemd5("../data/${each.value}")
}

resource "aws_dynamodb_table" "cenipa-etl-log" {
  name           = "cenipa-etl-log"
  billing_mode   = "PROVISIONED"
  read_capacity  = 10
  write_capacity = 10
  hash_key       = "object_id"
  range_key      = "timestamp"

  attribute {
    name = "object_id"
    type = "S"
  }

  attribute {
    name = "timestamp"
    type = "S"
  }
}

resource "aws_ecr_repository" "cenipa-etl" {
  name                 = "cenipa-etl"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}