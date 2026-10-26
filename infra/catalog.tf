# Glue Data Catalog, reached by Spark through Glue's Iceberg REST endpoint
# (the warehouse is the account id) and by Athena directly. Each database sets
# where its tables live, as Lakekeeper's warehouse does locally.
resource "aws_glue_catalog_database" "this" {
  for_each = toset(local.databases)

  name         = each.key
  description  = "ruc-lakehouse ${each.key} layer"
  location_uri = "s3://${aws_s3_bucket.this["lake"].id}/warehouse/${each.key}"
}

# Athena replaces Trino as the SQL engine for dbt and for analysts.
resource "aws_athena_workgroup" "lake" {
  name = var.project

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    # Athena bills by bytes scanned: stop runaway queries.
    bytes_scanned_cutoff_per_query = var.query_scan_limit_gb * 1024 * 1024 * 1024

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.this["ops"].id}/${local.athena_results_prefix}"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
