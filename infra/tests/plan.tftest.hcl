# Unit tests with a mocked AWS provider: no credentials, no network.
#   terraform test
# Runs apply against the mock, so computed values (ARNs, ids) are known.

mock_provider "aws" {
  override_data {
    target = data.aws_caller_identity.current
    values = { account_id = "123456789012" }
  }
  override_data {
    target = data.aws_partition.current
    values = { partition = "aws" }
  }
}

variables {
  alert_email = "alerts@example.com"
}

run "buckets_are_private_encrypted_and_tls_only" {
  command = apply

  assert {
    condition     = aws_s3_bucket.this["lake"].bucket == "ruc-lakehouse-lake-123456789012"
    error_message = "The lake bucket name must carry the account id."
  }
  assert {
    condition = alltrue([for block in aws_s3_bucket_public_access_block.this : alltrue([
      block.block_public_acls, block.block_public_policy, block.ignore_public_acls, block.restrict_public_buckets,
    ])])
    error_message = "Every bucket must block all public access."
  }
  assert {
    condition = alltrue([for config in aws_s3_bucket_server_side_encryption_configuration.this :
    one(one(config.rule).apply_server_side_encryption_by_default).sse_algorithm == "AES256"])
    error_message = "Every bucket must encrypt at rest."
  }
  assert {
    condition     = aws_s3_bucket_versioning.lake.versioning_configuration[0].status == "Enabled"
    error_message = "The lake bucket must be versioned: landing holds the only copy of each day."
  }
}

run "landing_is_archived_never_deleted" {
  command = apply

  assert {
    condition = [
      for transition in one([
        for rule in aws_s3_bucket_lifecycle_configuration.lake.rule : rule if rule.id == "archive-landing"
      ]).transition : transition.storage_class
    ] == ["GLACIER_IR"]
    error_message = "Landing must move to Glacier Instant Retrieval."
  }
  assert {
    condition = [
      for rule in aws_s3_bucket_lifecycle_configuration.lake.rule : rule.id if length(rule.expiration) > 0
    ] == ["expire-staging"]
    error_message = "Only staging may expire; landing is the raw history."
  }
  assert {
    condition = one([
      for rule in aws_s3_bucket_lifecycle_configuration.lake.rule : rule if rule.id == "expire-staging"
    ]).expiration[0].days == 7
    error_message = "Staging must expire after a week."
  }
  assert {
    condition = alltrue([
      for rule in aws_s3_bucket_lifecycle_configuration.lake.rule :
      length(rule.noncurrent_version_expiration) == 1 if rule.id != "abort-incomplete-uploads"
    ])
    error_message = "Every prefix must bound how long old versions are kept."
  }
}

run "glue_has_a_database_per_layer_under_the_warehouse" {
  command = apply

  assert {
    condition     = toset(keys(aws_glue_catalog_database.this)) == toset(["bronze", "silver", "ops", "staging", "gold", "reference"])
    error_message = "Expected one database per namespace used by Spark and dbt."
  }
  assert {
    condition     = aws_glue_catalog_database.this["silver"].location_uri == "s3://${aws_s3_bucket.this["lake"].id}/warehouse/silver"
    error_message = "Tables must live under warehouse/<database>."
  }
}

run "athena_caps_every_query" {
  command = apply

  assert {
    condition     = aws_athena_workgroup.lake.configuration[0].enforce_workgroup_configuration
    error_message = "Clients must not override the workgroup settings."
  }
  assert {
    condition     = aws_athena_workgroup.lake.configuration[0].bytes_scanned_cutoff_per_query == 10 * 1024 * 1024 * 1024
    error_message = "Queries must stop at 10 GiB scanned."
  }
}

run "emr_stops_when_idle_and_uses_the_stock_image_by_default" {
  command = apply

  assert {
    condition     = aws_emrserverless_application.spark.auto_stop_configuration[0].idle_timeout_minutes == 5
    error_message = "The application must stop after 5 idle minutes."
  }
  assert {
    condition     = aws_emrserverless_application.spark.maximum_capacity[0].cpu == "16 vCPU"
    error_message = "The application must have a capacity ceiling."
  }
  assert {
    condition     = length(aws_emrserverless_application.spark.image_configuration) == 0
    error_message = "Without job_image_tag the stock EMR image must be used."
  }
}

run "emr_uses_the_job_image_when_tagged" {
  command = apply

  variables {
    job_image_tag = "v1"
  }

  assert {
    condition     = endswith(aws_emrserverless_application.spark.image_configuration[0].image_uri, ":v1")
    error_message = "The application must use the tagged job image."
  }
}

run "spark_jobs_cannot_write_landing_or_dbt_layers" {
  command = apply

  assert {
    condition = alltrue(flatten([
      for statement in jsondecode(aws_iam_role_policy.emr_job.policy).Statement : [
        for resource in flatten([statement.Resource]) :
        !strcontains(resource, "/landing/") && !strcontains(resource, "/warehouse/gold/")
        if contains(flatten([statement.Action]), "s3:PutObject")
      ]
    ]))
    error_message = "The Spark job role must not write landing or gold."
  }
  assert {
    condition = alltrue([
      for statement in jsondecode(aws_iam_policy.orchestrator.policy).Statement :
      statement.Condition["StringEquals"]["iam:PassedToService"] == "emr-serverless.amazonaws.com"
      if statement.Action == "iam:PassRole"
    ])
    error_message = "The orchestrator may pass the job role to EMR Serverless only."
  }
}

run "budget_alerts_before_and_at_the_limit" {
  command = apply

  assert {
    condition     = aws_budgets_budget.monthly.limit_amount == "15.00"
    error_message = "Unexpected budget amount."
  }
  assert {
    condition = toset([for n in aws_budgets_budget.monthly.notification : "${n.notification_type}:${n.threshold}"]) == toset([
      "ACTUAL:80", "FORECASTED:100",
    ])
    error_message = "Expected alerts at 80% actual and 100% forecasted spend."
  }
}

run "rejects_an_unknown_architecture" {
  command = plan

  variables {
    emr_architecture = "SPARC"
  }

  expect_failures = [var.emr_architecture]
}
