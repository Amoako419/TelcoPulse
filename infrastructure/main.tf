terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "eu-west-1"
}

# Variables
variable "environment" {
  description = "Environment name (dev or prod)"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "Environment must be either 'dev' or 'prod'."
  }
}

variable "kinesis_stream_name" {
  description = "Name of the Kinesis Data Stream"
  type        = string
  default     = "telcopulse-stream"
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket for data storage"
  type        = string
  default     = "telcopulse-data"
}

# Kinesis Data Stream
resource "aws_kinesis_stream" "telcopulse_stream" {
  name             = "${var.environment}-${var.kinesis_stream_name}"
  shard_count      = 1
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Environment = var.environment
    Project     = "TelcoPulse"
  }
}

# S3 Bucket
resource "aws_s3_bucket" "telcopulse_bucket" {
  bucket = "${var.environment}-${var.s3_bucket_name}"

  tags = {
    Environment = var.environment
    Project     = "TelcoPulse"
  }
}

# S3 Bucket Versioning
resource "aws_s3_bucket_versioning" "telcopulse_bucket_versioning" {
  bucket = aws_s3_bucket.telcopulse_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 Bucket Lifecycle Configuration
resource "aws_s3_bucket_lifecycle_configuration" "telcopulse_bucket_lifecycle" {
  bucket = aws_s3_bucket.telcopulse_bucket.id

  rule {
    id     = "DeleteOldVersions"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

# S3 Bucket Public Access Block
resource "aws_s3_bucket_public_access_block" "telcopulse_bucket_access" {
  bucket = aws_s3_bucket.telcopulse_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Bucket Policy
resource "aws_s3_bucket_policy" "telcopulse_bucket_policy" {
  bucket = aws_s3_bucket.telcopulse_bucket.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnforceSSLOnly"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.telcopulse_bucket.arn,
          "${aws_s3_bucket.telcopulse_bucket.arn}/*"
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      }
    ]
  })
}

# Outputs
output "kinesis_stream_name" {
  description = "Name of the Kinesis Data Stream"
  value       = aws_kinesis_stream.telcopulse_stream.name
}

output "kinesis_stream_arn" {
  description = "ARN of the Kinesis Data Stream"
  value       = aws_kinesis_stream.telcopulse_stream.arn
}

output "s3_bucket_name" {
  description = "Name of the S3 bucket"
  value       = aws_s3_bucket.telcopulse_bucket.id
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.telcopulse_bucket.arn
} 