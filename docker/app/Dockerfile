# Project image: Python 3.12, Java 21 and PySpark with the Iceberg and S3A jars inside,
# so jobs start without downloading anything.
FROM python:3.12-slim-trixie

# Keep in step with src/ruc_lakehouse/spark.py (a test checks it).
ARG ICEBERG_VERSION=1.12.0
ARG SPARK_MINOR=4.1
ARG SCALA_VERSION=2.13
ARG HADOOP_VERSION=3.4.2
ARG AWS_SDK_VERSION=2.54.17

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.13 /uv /usr/local/bin/uv

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencies first, so code changes do not reinstall them.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-install-project

RUN jars=$(python -c "import pyspark, pathlib; print(pathlib.Path(pyspark.__file__).parent / 'jars')") \
    && fetch() { curl -fsSL -o "$jars/$2-$3.jar" "https://repo1.maven.org/maven2/$1/$2/$3/$2-$3.jar"; } \
    && fetch org/apache/iceberg "iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_VERSION}" "$ICEBERG_VERSION" \
    && fetch org/apache/iceberg iceberg-aws-bundle "$ICEBERG_VERSION" \
    && fetch org/apache/hadoop hadoop-aws "$HADOOP_VERSION" \
    && fetch software/amazon/awssdk s3-transfer-manager "$AWS_SDK_VERSION" \
    && fetch software/amazon/awssdk apache-client "$AWS_SDK_VERSION"

COPY src ./src
RUN uv sync --locked --no-dev
COPY dbt ./dbt

# dbt writes its artifacts outside the read-only project folder.
ENV DBT_PROJECT_DIR=/app/dbt \
    DBT_PROFILES_DIR=/app/dbt \
    DBT_TARGET_PATH=/tmp/dbt/target \
    DBT_LOG_PATH=/tmp/dbt/logs

RUN useradd --create-home --uid 1000 lake
USER lake
