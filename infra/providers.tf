provider "aws" {
  region = var.region

  # Only for S3 emulators (Moto in CI); real S3 uses virtual-hosted URLs.
  s3_use_path_style = var.s3_use_path_style

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}
