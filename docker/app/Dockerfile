# Project image: Python 3.12, Java 21 and PySpark with the Iceberg jars inside,
# so jobs start without downloading anything.
FROM python:3.12-slim-trixie

ARG ICEBERG_VERSION=1.12.0
ARG SPARK_MINOR=4.1
ARG SCALA_VERSION=2.13

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
    && base=https://repo1.maven.org/maven2/org/apache/iceberg \
    && curl -fsSL -o "$jars/iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
        "$base/iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" \
    && curl -fsSL -o "$jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
        "$base/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

COPY src ./src
RUN uv sync --locked --no-dev

RUN useradd --create-home --uid 1000 lake
USER lake
