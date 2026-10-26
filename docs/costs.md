# What it would cost on AWS

An estimate for the deployment in [`infra/`](../infra/), after a year of daily
snapshots. Prices are AWS's public on-demand list prices for us-east-1, taken
from the AWS Price List API in October 2026. Volumes come from the real
2026-10-25 snapshot.

**About US$9 a month on x86, under US$8 on Graviton (ARM64).** Nearly all of it
is Spark compute, and nothing runs between jobs.

| Item | Assumption | Per month |
|---|---|---|
| EMR Serverless, x86 | 64 job runs (bronze and silver daily, maintenance weekly), each holding the full ceiling of 16 vCPU and 64 GB for 5 minutes | $6.46 |
| S3, landing | 377 MB a day; the last 30 days in Standard, the other ~124 GB in Glacier Instant Retrieval | $0.76 |
| S3, staging | 440 MB a day, kept 7 days plus 1 as an old version | $0.08 |
| S3, bronze | 310 MB a day; 15 days kept, plus up to 14 more until maintenance and versioning remove them | $0.21 |
| S3, silver, gold and ops | ~0.5 GB live, plus two weeks of replaced versions | $0.06 |
| S3 requests | A few thousand writes a day | < $0.50 |
| Athena | `dbt build` scans about 2 GB a day | $0.30 |
| ECR | Job image, about 3 GB (EMR's base layers) | $0.30 |
| Glue Data Catalog | About 20 tables and a few thousand requests a day, inside the free first million objects and requests | $0 |
| AWS Budgets | One budget | $0 |
| **Total** | | **≈ $8.70** |

## How the numbers were worked out

| Price (us-east-1) | x86 | Graviton |
|---|---|---|
| EMR Serverless, per vCPU-hour | $0.052624 | $0.042094 |
| EMR Serverless, per GB-hour | $0.0057785 | $0.004628 |
| S3 Standard, per GB-month | $0.023 | |
| S3 Glacier Instant Retrieval, per GB-month (plus $0.03 per GB read) | $0.004 | |
| Athena, per TB scanned | $5.00 | |
| ECR, per GB-month | $0.10 | |

One job run at the ceiling for 5 minutes uses 1.33 vCPU-hours and 5.33
GB-hours: $0.101 on x86, $0.081 on Graviton. On a laptop the same jobs take
about 2 minutes (bronze) and 1.5 minutes (silver), so the 5 minutes, which
include the time workers take to start, are an upper bound.

## What moves the total

- **Compute.** Graviton saves about 20%; the architecture is one variable
  (`emr_architecture`). Running bronze and silver in one job run would pay the
  start-up once, and smaller executors would lower the ceiling the jobs reach.
- **Landing is the only line that keeps growing,** by about US$0.05 each month,
  because it is never deleted. Moving zips older than a year to Glacier Deep
  Archive would cut that by three quarters, at the cost of a 12-hour restore.
- **Bronze retention** (`--bronze-days`) and **time travel** (`--keep-days`)
  trade storage for the ability to re-run recent days.

## Not included

- Where Dagster runs (ECS or Dagster+) and its database: an always-on service
  that would likely cost more than everything above.
- The daily download on AWS (a scheduled Lambda or ECS task), which would cost
  cents.
- Data transfer: everything stays in one region, and downloads from SUNAT come
  in for free.
