# ruc-lakehouse

Cloud-ready lakehouse that turns SUNAT's daily taxpayer registry snapshots
(*padrón reducido RUC*) into history.

SUNAT publishes only the current state of every taxpayer: one ~390 MB zip,
replaced every day. This project keeps each snapshot, compares it with the
previous one and stores the changes as Iceberg tables with full history
(SCD type 2): new taxpayers, deregistrations, and changes of status, address
condition or location.

> **Status:** phase 1 of 7, the local stack. See [Roadmap](#roadmap).

## Architecture

Everything runs locally in Docker, but only through interfaces AWS offers as
managed services, so moving to the cloud is a configuration change:

| Local service | Role | AWS equivalent |
|---|---|---|
| Silo (S3-compatible storage) | Data lake files | Amazon S3 |
| Lakekeeper | Iceberg REST catalog | AWS Glue Data Catalog (Iceberg REST endpoint) |
| PySpark 4.1 + Iceberg 1.12 | Ingestion and history (bronze, silver) | EMR Serverless |
| Trino | SQL engine | Amazon Athena |
| Postgres | Catalog and orchestrator metadata only | Amazon RDS |

The active target is chosen with `LAKE_PROFILE` (`local` or `aws`); see
[`src/ruc_lakehouse/config.py`](src/ruc_lakehouse/config.py).

## Quick start

Requirements: Docker with about 6 GB of free memory.

```bash
cp .env.example .env          # then change the secrets
docker compose up -d --build --wait
docker compose run --rm jobs python -m ruc_lakehouse.jobs.smoke
docker compose exec trino trino --execute "select * from lakehouse.smoke.ping"
```

| Service | URL |
|---|---|
| Storage console | http://localhost:9001 |
| Catalog API | http://localhost:8181 |
| Trino | http://localhost:8080 |
| Spark UI (while a job runs) | http://localhost:4040 |

## Development

```bash
uv sync
uv run pre-commit install
uv run pytest
uv run ruff check . && uv run ruff format --check .
```

## Roadmap

- [x] Phase 1: local stack (storage, catalog, Spark, Trino) and CI
- [ ] Phase 2: bronze, snapshots loaded as Iceberg tables with a schema contract
- [ ] Phase 3: silver, daily diff and SCD type 2 history
- [ ] Phase 4: gold, dbt models on Trino with data tests
- [ ] Phase 5: orchestration with Dagster (daily partitions, backfills)
- [ ] Phase 6: AWS infrastructure as code with Terraform
- [ ] Phase 7: table maintenance, observability and documentation

## Data source

[SUNAT, padrón reducido RUC](https://www.sunat.gob.pe/descargaPRR/mrc137_padron_reducido.html),
public data published by Peru's tax administration.

## License

[MIT](LICENSE)
