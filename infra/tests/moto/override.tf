# Copied into infra/ for the Moto run in CI; never used against AWS.

terraform {
  backend "local" {}
}

provider "aws" {
  # Fail fast on an API the emulator lacks instead of retrying for minutes.
  max_retries = 1
}

# Moto does not implement the Budgets subscriber API that the provider reads
# back after creating a notification. The budget is covered by the mocked
# tests in tests/plan.tftest.hcl instead.
resource "aws_budgets_budget" "monthly" {
  count = 0
}

# Attributes Moto accepts but does not return, so the second plan would show
# them as changes. Everything else must come back exactly as applied.
resource "aws_emrserverless_application" "spark" {
  lifecycle {
    ignore_changes = [architecture, image_configuration, monitoring_configuration, runtime_configuration]
  }
}

resource "aws_s3_bucket" "this" {
  lifecycle {
    ignore_changes = [tags_all]
  }
}
