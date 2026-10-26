# Least privilege per role:
#   emr_job       the Spark jobs: read staging, write the bronze, silver and ops
#                 tables. Cannot touch landing or the dbt layers.
#   orchestrator  whatever runs Dagster: land files, start the Spark jobs, and
#                 run dbt on Athena (read silver, write staging/gold/reference).
# Dagster's own hosting (ECS or Dagster+) is outside this configuration, so the
# orchestrator permissions are a managed policy to attach to its role.

locals {
  lake_arn = aws_s3_bucket.this["lake"].arn
  ops_arn  = aws_s3_bucket.this["ops"].arn

  glue_arn = "arn:${local.partition}:glue:${var.region}:${local.account_id}"

  glue_read = ["glue:GetCatalog", "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables"]
  glue_write = [
    "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable",
    "glue:GetPartitions", "glue:BatchCreatePartition", "glue:BatchDeletePartition",
  ]
  s3_read  = ["s3:GetObject"]
  s3_write = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
}

# Glue resources for a set of databases: the catalog, each database and its tables.
locals {
  glue_tables = {
    for name, dbs in {
      job       = local.job_databases
      transform = local.transform_databases
      silver    = ["silver"]
    } :
    name => concat(
      ["${local.glue_arn}:catalog"],
      [for db in dbs : "${local.glue_arn}:database/${db}"],
      [for db in dbs : "${local.glue_arn}:table/${db}/*"],
    )
  }
}

resource "aws_iam_role" "emr_job" {
  name = "${var.project}-emr-job"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = { "aws:SourceAccount" = local.account_id }
        ArnLike      = { "aws:SourceArn" = aws_emrserverless_application.spark.arn }
      }
    }]
  })
}

resource "aws_iam_role_policy" "emr_job" {
  name = "lakehouse-jobs"
  role = aws_iam_role.emr_job.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListLake"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = local.lake_arn
      },
      {
        Sid      = "ReadStaging"
        Effect   = "Allow"
        Action   = local.s3_read
        Resource = "${local.lake_arn}/staging/*"
      },
      {
        Sid      = "WriteJobTables"
        Effect   = "Allow"
        Action   = local.s3_write
        Resource = [for db in local.job_databases : "${local.lake_arn}/warehouse/${db}/*"]
      },
      {
        Sid      = "JobCatalog"
        Effect   = "Allow"
        Action   = concat(local.glue_read, local.glue_write)
        Resource = local.glue_tables.job
      },
      {
        Sid      = "WriteLogs"
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "${local.ops_arn}/${local.emr_logs_prefix}*"
      },
    ]
  })
}

resource "aws_iam_policy" "orchestrator" {
  name        = "${var.project}-orchestrator"
  description = "Land SUNAT files, run the Spark jobs on EMR Serverless and dbt on Athena."
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListBuckets"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [local.lake_arn, local.ops_arn]
      },
      {
        Sid      = "LandFiles"
        Effect   = "Allow"
        Action   = local.s3_write
        Resource = ["${local.lake_arn}/landing/*", "${local.lake_arn}/staging/*"]
      },
      {
        Sid    = "RunSparkJobs"
        Effect = "Allow"
        Action = [
          "emr-serverless:GetApplication", "emr-serverless:StartApplication",
          "emr-serverless:StartJobRun", "emr-serverless:GetJobRun",
          "emr-serverless:CancelJobRun", "emr-serverless:GetDashboardForJobRun",
        ]
        Resource = [aws_emrserverless_application.spark.arn, "${aws_emrserverless_application.spark.arn}/jobruns/*"]
      },
      {
        Sid       = "PassJobRole"
        Effect    = "Allow"
        Action    = "iam:PassRole"
        Resource  = aws_iam_role.emr_job.arn
        Condition = { StringEquals = { "iam:PassedToService" = "emr-serverless.amazonaws.com" } }
      },
      {
        Sid      = "ReadJobLogs"
        Effect   = "Allow"
        Action   = local.s3_read
        Resource = "${local.ops_arn}/${local.emr_logs_prefix}*"
      },
      {
        Sid    = "RunQueries"
        Effect = "Allow"
        Action = [
          "athena:GetWorkGroup", "athena:StartQueryExecution", "athena:StopQueryExecution",
          "athena:GetQueryExecution", "athena:GetQueryResults",
        ]
        Resource = aws_athena_workgroup.lake.arn
      },
      {
        Sid      = "QueryResults"
        Effect   = "Allow"
        Action   = local.s3_write
        Resource = "${local.ops_arn}/${local.athena_results_prefix}*"
      },
      {
        Sid      = "ReadSilver"
        Effect   = "Allow"
        Action   = local.glue_read
        Resource = local.glue_tables.silver
      },
      {
        Sid      = "ReadSilverFiles"
        Effect   = "Allow"
        Action   = local.s3_read
        Resource = "${local.lake_arn}/warehouse/silver/*"
      },
      {
        Sid      = "TransformCatalog"
        Effect   = "Allow"
        Action   = concat(local.glue_read, local.glue_write)
        Resource = local.glue_tables.transform
      },
      {
        Sid      = "WriteTransformTables"
        Effect   = "Allow"
        Action   = local.s3_write
        Resource = [for db in local.transform_databases : "${local.lake_arn}/warehouse/${db}/*"]
      },
    ]
  })
}
