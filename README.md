# ruc-lakehouse

Cloud-ready lakehouse that turns SUNAT's daily taxpayer registry snapshots
(*padrón reducido RUC*) into history.

SUNAT publishes only the current state of every taxpayer: one ~390 MB zip,
replaced every day. This project keeps each snapshot, compares it with the
previous one and stores the changes as Iceberg tables with full history
(SCD type 2): new taxpayers, deregistrations, and changes of status, address
condition or location.

> **Status:** phase 4 of 7, gold models. See [Roadmap](#roadmap).

## Architecture

Everything runs locally in Docker, but only through interfaces AWS offers as
managed services, so moving to the cloud is a configuration change:

| Local service | Role | AWS equivalent |
|---|---|---|
| Silo (S3-compatible storage) | Data lake files | Amazon S3 |
| Lakekeeper | Iceberg REST catalog | AWS Glue Data Catalog (Iceberg REST endpoint) |
| PySpark 4.1 + Iceberg 1.12 | Ingestion and history (bronze, silver) | EMR Serverless |
| Trino | SQL engine | Amazon Athena |
| dbt (dbt-trino) | Gold models and data tests | dbt-athena |
| Postgres | Catalog and orchestrator metadata only | Amazon RDS |

The active target is chosen with `LAKE_PROFILE` (`local` or `aws`); see
[`src/ruc_lakehouse/config.py`](src/ruc_lakehouse/config.py).

## Data flow

| Step | Where | What happens |
|---|---|---|
| Collect | outside the repo | A daily task keeps one zip per day, since SUNAT keeps no history |
| Landing | `s3://…/landing/` | The zip exactly as published, with its SHA-256. The header is checked against the [contract](src/ruc_lakehouse/contracts.py), so a schema change stops here |
| Staging | `s3://…/staging/` | The same lines re-chunked into UTF-8 gzip parts: Spark cannot split a zip and would read 1.6 GB in one task |
| Bronze | `bronze.padron_ruc` | One row per valid line, partitioned by snapshot date and sorted by RUC. Broken lines go to `bronze.padron_ruc_rejects` with the reason |
| Silver | `silver.taxpayers_history` | One row per version of each RUC (SCD type 2): `valid_from`, `valid_to`, `is_current`, why it opened (`initial`, `new`, `changed`), which columns changed, and why it closed (`changed`, `removed`) |

Every load reconciles its counts with the landing marker and stops if more than
0.1% of the lines are rejected; each run is recorded in `ops.job_runs`.
Landing and bronze can be re-run safely: the same file is skipped, and bronze
replaces the snapshot's partition.

Silver compares each day with the current versions through a hash of the
attributes and applies only new, changed and removed RUCs in one `MERGE`. Days
are applied in order (the same day again is a no-op, an earlier one is
refused), and a day that would remove more than 1% of the registry is refused
as a probable truncated file. Two design choices keep the daily cost low:

- **Merge-on-read.** Changes land on scattered RUCs, so copy-on-write would
  rewrite nearly every file each day; merge-on-read writes the changes and a
  small delete file, and compaction merges them later.
- **No stored hash.** A SHA-256 per row does not compress: storing it made the
  table 977 MB instead of 323 MB. It is recomputed on both sides at run time.

One real snapshot (2026-10-25): **18,486,442 lines** in a 394 MB zip become
310 MB of Parquet in bronze in about two minutes on a laptop, and the initial
silver load takes another minute and a half. Only one line is rejected
(an `AAAAAAAAAAA` RUC). SUNAT escapes the delimiter inside values (`\|`, and
`\\` for a backslash), as in `EBEN EZER \| INGENIERIA Y ARQUITECTURA S.A.C.`;
a naive split on `|` would lose those taxpayers.

## Gold models

dbt builds the reporting layer on Trino (`gold` schema), with
[INEI's 2022 list of 1,891 districts](https://www.datosabiertos.gob.pe/dataset/ubigeos-c%C3%B3digos-de-ubicaci%C3%B3n-geogr%C3%A1fica-instituto-nacional-de-estad%C3%ADstica-e-inform%C3%A1tica-inei)
as a seed:

| Model | Grain |
|---|---|
| `dim_taxpayer` | One row per RUC in its current state, with taxpayer type (natural person or legal entity, from the RUC prefix), status group and district |
| `dim_ubigeo` | One row per district |
| `fct_taxpayer_events` | One row per change: registered, removed, or a change of status, address condition, location, name or address, with the value before and after |
| `agg_registry_daily` | Registry size on every snapshot date by type, status, address condition and department |
| `agg_daily_events` | Events per day by type and department |
| `agg_status_transitions` | Daily moves between statuses, e.g. `ACTIVO` to `BAJA DE OFICIO` |

`agg_registry_daily` does not re-read the whole history for every date: each
version adds one to its group on `valid_from` and subtracts one on `valid_to`,
and a running sum gives the size on each date.

Tests cover keys, accepted values, the link to the district list and three
custom checks: one current version per RUC, the daily registry adding up to
the current versions, and no names of natural persons in gold, which only
names legal entities. The district check is a warning: 78 taxpayers carry
codes of districts reorganized after 2016, mostly in Loreto.

On the real snapshot, `dbt build` takes about 40 seconds. Of the 2.37 million
legal entities in the registry, 1.38 million are deregistered and 890,000 are
active, half of them in Lima.

## Quick start

Requirements: Docker with about 6 GB of free memory.

```bash
cp .env.example .env          # then change the secrets
docker compose up -d --build --wait
docker compose run --rm jobs python -m ruc_lakehouse.jobs.smoke
```

Load one snapshot. `SNAPSHOTS_HOST_DIR` in `.env` points at the folder with
`raw/snapshot_date=YYYY-MM-DD/padron_reducido_ruc.zip`; to try it without the
real file, generate a synthetic one:

```bash
uv run python tests/fixtures/make_snapshot.py data/snapshots 2026-01-15
docker compose run --rm jobs python -m ruc_lakehouse.ingest.landing 2026-01-15
docker compose run --rm jobs python -m ruc_lakehouse.jobs.bronze 2026-01-15
docker compose run --rm jobs python -m ruc_lakehouse.jobs.silver 2026-01-15
docker compose run --rm jobs dbt build
docker compose exec trino trino --execute "select * from lakehouse.gold.agg_registry_daily"
```

For a second day with known changes (30 removed, 30 renamed, 20 new), generate
`2026-01-16 3000 1`, run the same three jobs for that date and `dbt build`.

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
uv run sqlfluff lint dbt/models dbt/tests
uv run dbt parse --project-dir dbt --profiles-dir dbt
```

The district seed is rebuilt from INEI's file with
`uv run python -m ruc_lakehouse.reference.ubigeo dbt/seeds/ubigeo.csv`.

## Roadmap

- [x] Phase 1: local stack (storage, catalog, Spark, Trino) and CI
- [x] Phase 2: bronze, snapshots loaded as Iceberg tables with a schema contract
- [x] Phase 3: silver, daily diff and SCD type 2 history
- [x] Phase 4: gold, dbt models on Trino with data tests
- [ ] Phase 5: orchestration with Dagster (daily partitions, backfills)
- [ ] Phase 6: AWS infrastructure as code with Terraform
- [ ] Phase 7: table maintenance, observability and documentation

## Data source

[SUNAT, padrón reducido RUC](https://www.sunat.gob.pe/descargaPRR/mrc137_padron_reducido.html),
public data published by Peru's tax administration. District names:
INEI, *Ubigeos 2022* (ODbL).

## License

[MIT](LICENSE)
