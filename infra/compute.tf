# Spark jobs (bronze, silver) on EMR Serverless, from the job image in ECR
# (docker/emr/Dockerfile). No workers run, and nothing is billed, between jobs.

resource "aws_ecr_repository" "jobs" {
  name                 = "${var.project}/jobs"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "jobs" {
  repository = aws_ecr_repository.jobs.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the last 10 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

# EMR Serverless pulls the image itself, so the repository must let it in.
resource "aws_ecr_repository_policy" "jobs" {
  repository = aws_ecr_repository.jobs.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "EmrServerlessPull"
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
      Action    = ["ecr:BatchGetImage", "ecr:DescribeImages", "ecr:GetDownloadUrlForLayer"]
      Condition = {
        ArnLike = {
          "aws:SourceArn" = "arn:${local.partition}:emr-serverless:${var.region}:${local.account_id}:/applications/*"
        }
      }
    }]
  })
}

resource "aws_emrserverless_application" "spark" {
  name          = var.project
  release_label = var.emr_release_label
  type          = "SPARK"
  architecture  = var.emr_architecture

  # A hard ceiling on what all running jobs may use together.
  maximum_capacity {
    cpu    = "${var.emr_max_vcpu} vCPU"
    memory = "${var.emr_max_memory_gb} GB"
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 5
  }

  dynamic "image_configuration" {
    for_each = var.job_image_tag == null ? [] : [var.job_image_tag]
    content {
      image_uri = "${aws_ecr_repository.jobs.repository_url}:${image_configuration.value}"
    }
  }

  # The runtime runs PySpark on Python 3.11 by default; the job image installs
  # the package for 3.12. Job runs add their own settings on top of these.
  runtime_configuration {
    classification = "spark-defaults"
    properties = {
      "spark.emr-serverless.driverEnv.PYSPARK_PYTHON"        = "/usr/bin/python3.12"
      "spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON" = "/usr/bin/python3.12"
      "spark.executorEnv.PYSPARK_PYTHON"                     = "/usr/bin/python3.12"
    }
  }

  monitoring_configuration {
    s3_monitoring_configuration {
      log_uri = "s3://${aws_s3_bucket.this["ops"].id}/${local.emr_logs_prefix}"
    }
  }

  depends_on = [aws_ecr_repository_policy.jobs]
}
