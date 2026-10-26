# AWS deployment of the lakehouse. Each resource replaces a local service:
#   S3 buckets             <- Silo (S3-compatible storage)
#   Glue Data Catalog      <- Lakekeeper (Iceberg REST catalog)
#   EMR Serverless         <- the Spark jobs container
#   Athena                 <- Trino
# The jobs pick the AWS services with LAKE_PROFILE=aws (see the lake_env output).

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition

  # Bucket names are global, so they carry the account id.
  lake_bucket = "${var.project}-lake-${local.account_id}"
  ops_bucket  = "${var.project}-ops-${local.account_id}"

  # Iceberg namespaces, one Glue database each. Spark writes the first three,
  # dbt the rest.
  job_databases       = ["bronze", "silver", "ops"]
  transform_databases = ["staging", "gold", "reference"]
  databases           = concat(local.job_databases, local.transform_databases)

  athena_results_prefix = "athena-results/"
  emr_logs_prefix       = "emr-logs/"
}
