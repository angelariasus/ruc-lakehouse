# Two buckets:
#   lake  landing/ (zips as published), staging/ (gzip parts) and warehouse/
#         (Iceberg tables). Versioned: landing is the only copy of each day.
#   ops   Athena results and EMR logs, disposable, so not versioned.

locals {
  buckets = {
    lake = local.lake_bucket
    ops  = local.ops_bucket
  }
}

resource "aws_s3_bucket" "this" {
  for_each = local.buckets

  bucket = each.value
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = aws_s3_bucket.this

  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [each.value.arn, "${each.value.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })

  depends_on = [aws_s3_bucket_public_access_block.this]
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.this["lake"].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.this["lake"].id

  # Versioning keeps every deleted or overwritten object as a noncurrent
  # version, so each prefix also says how long to keep those.

  # The zips are the raw history: keep them, but in a cheaper class once the
  # day has been loaded. Instant Retrieval still allows a re-run at any time.
  # A zip deleted or overwritten by mistake stays recoverable for a while.
  rule {
    id     = "archive-landing"
    status = "Enabled"
    filter {
      prefix = "landing/"
    }
    transition {
      days          = var.landing_archive_days
      storage_class = "GLACIER_IR"
    }
    noncurrent_version_expiration {
      noncurrent_days = var.landing_version_days
    }
  }

  # Rebuilt from landing whenever needed: no point keeping old versions.
  rule {
    id     = "expire-staging"
    status = "Enabled"
    filter {
      prefix = "staging/"
    }
    expiration {
      days = var.staging_expiry_days
    }
    noncurrent_version_expiration {
      noncurrent_days = 1
    }
  }

  # Files deleted by snapshot expiry (src/ruc_lakehouse/jobs/maintenance.py),
  # kept one more week as a safety net.
  rule {
    id     = "expire-old-table-files"
    status = "Enabled"
    filter {
      prefix = "warehouse/"
    }
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.lake]
}

resource "aws_s3_bucket_lifecycle_configuration" "ops" {
  bucket = aws_s3_bucket.this["ops"].id

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = local.athena_results_prefix
    }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "expire-emr-logs"
    status = "Enabled"
    filter {
      prefix = local.emr_logs_prefix
    }
    expiration {
      days = 30
    }
  }
}
