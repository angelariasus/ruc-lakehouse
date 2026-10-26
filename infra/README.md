# AWS infrastructure

Terraform for running the lakehouse on AWS. **Defined, not deployed:** CI
lints it, unit-tests it with a mocked provider and applies it against an AWS
API emulator, but it has not run against a real account.

## What it creates

| Resource | Replaces locally | Purpose |
|---|---|---|
| S3 bucket `…-lake-<account>` | Silo | `landing/`, `staging/` and the Iceberg `warehouse/` |
| S3 bucket `…-ops-<account>` | | Athena results and EMR logs, both expiring |
| Glue databases `bronze`, `silver`, `ops`, `staging`, `gold`, `reference` | Lakekeeper | Iceberg catalog, reached by Spark through Glue's Iceberg REST endpoint |
| Athena workgroup | Trino | SQL engine for dbt and analysts |
| EMR Serverless application (`emr-spark-8.1.0`, Spark 4.1) | `jobs` container | Runs bronze and silver |
| ECR repository `…/jobs` | | Job image: [`docker/emr/Dockerfile`](../docker/emr/Dockerfile) |
| IAM role `…-emr-job`, policy `…-orchestrator` | | Least-privilege access for the jobs and for Dagster |
| Budget | | Email at 80% of the monthly limit and when the forecast passes it |

## Design choices

- **Landing is never deleted.** SUNAT keeps no history, so the zips are the
  only copy of each day. They move to Glacier Instant Retrieval after 30 days
  (about a sixth of the price) and stay readable at once for a re-run. The
  lake bucket is versioned, and each prefix bounds its old versions: 30 days
  for a zip deleted by mistake, 7 for table files removed by maintenance, 1 for
  staging. Staging can be rebuilt from landing, so it expires after a week.
- **Least privilege per role.** The Spark jobs read staging and write only the
  `bronze`, `silver` and `ops` tables. Dagster (the orchestrator policy) lands
  files, starts jobs, can pass the job role only to EMR Serverless, and runs dbt
  on Athena: it reads silver and writes `staging`, `gold` and `reference`. Tests
  check the important limits.
- **Pay only while working.** EMR Serverless starts on the first job and stops
  after 5 idle minutes, with a ceiling of 16 vCPU and 64 GB. Athena cancels any
  query that scans more than 10 GiB. There is no VPC: the jobs talk only to S3
  and Glue, so there is no NAT gateway to pay for.
- **The same code, another profile.** `terraform output lake_env` gives the
  variables for `LAKE_PROFILE=aws`: S3 with the IAM role, and Glue's Iceberg
  REST endpoint signed with SigV4 instead of Lakekeeper.
- **The job image is AWS's runtime plus the package.** It starts from EMR
  Serverless's own Spark 4.1 image and adds only the package, `boto3` and
  `dagster-pipes` for Python 3.12. Spark, Iceberg and S3A come with the runtime,
  and `spark.py` detects them.

## Checks without an AWS account

CI runs, from `infra/`:

```bash
terraform fmt -check -recursive
terraform init -backend=false && terraform validate
tflint --init && tflint
terraform test   # tests/plan.tftest.hcl, mocked provider
```

Then it applies the configuration against [Moto](https://github.com/getmoto/moto),
plans again (the plan must be empty) and destroys it. Moto falls short in three
places, handled in [`tests/moto/`](tests/moto/) and nowhere else:

- It does not implement the Budgets subscriber API, so the budget is left out
  of that run; the mocked tests cover it.
- It creates EMR Serverless applications as `STARTED` rather than `CREATED`, so
  a small wrapper starts the server with that fixed. It also ignores the image,
  runtime and log settings, and only accepts release labels such as
  `emr-7.12.0`.
- It does not return S3 bucket tags through S3 Control.

The job image can be checked locally too. It runs the smoke job with AWS's
Spark and Iceberg builds against the local catalog and storage:

```bash
docker build -f docker/emr/Dockerfile -t ruc-lakehouse-emr .
docker run --rm --network ruc-lakehouse_default --env-file .env \
  --entrypoint /usr/lib/spark/bin/spark-submit ruc-lakehouse-emr \
  --master 'local[2]' /opt/ruc-lakehouse/main.py ruc_lakehouse.jobs.smoke
```

## Deploying

With an account and a state bucket:

```bash
cp backend.hcl.example backend.hcl            # state bucket
cp terraform.tfvars.example terraform.tfvars  # alert email, region
terraform init -backend-config=backend.hcl
terraform apply

# Build and push the job image, then point the application at it.
repo=$(terraform output -raw job_image_repository)
aws ecr get-login-password | docker login --username AWS --password-stdin "${repo%%/*}"
docker build -f ../docker/emr/Dockerfile -t "$repo:v1" .. && docker push "$repo:v1"
terraform apply -var job_image_tag=v1
```

A job run names the module and the snapshot date, as locally:

```bash
aws emr-serverless start-job-run \
  --application-id "$(terraform output -raw emr_application_id)" \
  --execution-role-arn "$(terraform output -raw emr_job_role_arn)" \
  --job-driver '{"sparkSubmit": {
      "entryPoint": "local:///opt/ruc-lakehouse/main.py",
      "entryPointArguments": ["ruc_lakehouse.jobs.bronze", "2026-10-25"],
      "sparkSubmitParameters": "--conf spark.emr-serverless.driverEnv.LAKE_PROFILE=aws --conf spark.emr-serverless.driverEnv.LAKE_BUCKET=<lake bucket> --conf spark.emr-serverless.driverEnv.CATALOG_WAREHOUSE=<account id>"}}'
```

Not covered here yet:

- where Dagster runs (ECS or Dagster+), plus its Pipes client for EMR Serverless;
- the daily download on AWS (a scheduled Lambda or ECS task);
- a dbt-athena target.
