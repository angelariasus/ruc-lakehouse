terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.70"
    }
  }

  # Remote state in S3 with native locking. The bucket is created outside this
  # configuration; pass its settings at init time:
  #   terraform init -backend-config=backend.hcl
  backend "s3" {}
}
