output "lake_bucket" {
  description = "Bucket with landing, staging and the Iceberg warehouse."
  value       = aws_s3_bucket.this["lake"].id
}

output "ops_bucket" {
  description = "Bucket with Athena results and EMR logs."
  value       = aws_s3_bucket.this["ops"].id
}

output "athena_workgroup" {
  description = "Workgroup for dbt-athena and ad hoc queries."
  value       = aws_athena_workgroup.lake.name
}

output "emr_application_id" {
  description = "EMR Serverless application that runs the Spark jobs."
  value       = aws_emrserverless_application.spark.id
}

output "emr_job_role_arn" {
  description = "Execution role to pass to every EMR Serverless job run."
  value       = aws_iam_role.emr_job.arn
}

output "job_image_repository" {
  description = "ECR repository for the job image (docker/emr/Dockerfile)."
  value       = aws_ecr_repository.jobs.repository_url
}

output "orchestrator_policy_arn" {
  description = "Managed policy to attach to the role that runs Dagster."
  value       = aws_iam_policy.orchestrator.arn
}

output "lake_env" {
  description = "Environment for the jobs with LAKE_PROFILE=aws (see src/ruc_lakehouse/config.py)."
  value = {
    LAKE_PROFILE      = "aws"
    LAKE_BUCKET       = aws_s3_bucket.this["lake"].id
    AWS_REGION        = var.region
    CATALOG_WAREHOUSE = local.account_id
  }
}
