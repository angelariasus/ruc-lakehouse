-- One database per service on the same Postgres instance. The lakehouse data
-- itself lives in object storage, not here.
create database lakekeeper;
create database dagster;
