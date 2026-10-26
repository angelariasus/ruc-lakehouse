# Job image for EMR Serverless: AWS's Spark 4.1 runtime plus this package.
# Spark, Iceberg, Hadoop S3A and the AWS SDK come with the base image, so only
# the package and the libraries the jobs import are added.
#
#   docker build -f docker/emr/Dockerfile -t <account>.dkr.ecr.<region>.amazonaws.com/ruc-lakehouse/jobs:<tag> .
#
# The base image must match the application's release label (emr_release_label
# in infra/), or EMR Serverless refuses it.
ARG EMR_RELEASE=emr-spark-8.1.0
FROM public.ecr.aws/emr-serverless/spark4/${EMR_RELEASE}:latest

USER root

COPY --from=ghcr.io/astral-sh/uv:0.13 /uv /usr/local/bin/uv

ENV UV_PYTHON=/usr/bin/python3.12 \
    UV_SYSTEM_PYTHON=1 \
    UV_COMPILE_BYTECODE=1 \
    PYSPARK_PYTHON=/usr/bin/python3.12 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3.12

WORKDIR /opt/ruc-lakehouse

# Locked versions of the job dependencies, then the package without the rest
# (Dagster, dbt and PySpark stay out: the runtime brings its own Spark).
COPY pyproject.toml uv.lock README.md ./
RUN uv export --frozen --only-group job --no-emit-project --no-hashes -o requirements.txt \
    && uv pip install -r requirements.txt
COPY src ./src
RUN uv pip install --no-deps . && rm -rf src

COPY docker/emr/main.py ./main.py

# EMR Serverless runs the image as hadoop.
USER hadoop:hadoop
